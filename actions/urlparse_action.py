"""URL parsing and manipulation action module for RabAI AutoClick.

Provides URL operations:
- UrlParseAction: Parse URL components
- UrlBuildAction: Build URL from components
- UrlJoinAction: Join URL with path
- UrlEncodeAction: URL encode string
- UrlDecodeAction: URL decode string
- UrlValidateAction: Validate URL
"""

from __future__ import annotations

import sys
from urllib.parse import urlparse, urlunparse, urljoin, quote, unquote, urlencode, parse_qs, urlunsplit
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UrlParseAction(BaseAction):
    """Parse URL components."""
    action_type = "url_parse"
    display_name = "URL解析"
    description = "解析URL组件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL parse."""
        url = params.get('url', '')
        output_var = params.get('output_var', 'url_parsed')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            resolved = context.resolve_value(url) if context else url

            parsed = urlparse(resolved)
            result = {
                'scheme': parsed.scheme,
                'netloc': parsed.netloc,
                'hostname': parsed.hostname,
                'port': parsed.port,
                'path': parsed.path,
                'params': parsed.params,
                'query': parsed.query,
                'fragment': parsed.fragment,
                'username': parsed.username,
                'password': parsed.password,
            }

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Parsed {resolved}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"URL parse error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_parsed'}


class UrlBuildAction(BaseAction):
    """Build URL from components."""
    action_type = "url_build"
    display_name = "URL构建"
    description = "构建URL"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL build."""
        scheme = params.get('scheme', 'https')
        host = params.get('host', '')
        path = params.get('path', '')
        query = params.get('query', None)  # dict or string
        fragment = params.get('fragment', '')
        port = params.get('port', None)
        output_var = params.get('output_var', 'url_built')

        if not host:
            return ActionResult(success=False, message="host is required")

        try:
            resolved_scheme = context.resolve_value(scheme) if context else scheme
            resolved_host = context.resolve_value(host) if context else host
            resolved_path = context.resolve_value(path) if context else path
            resolved_fragment = context.resolve_value(fragment) if context else fragment
            resolved_port = context.resolve_value(port) if context else port

            netloc = resolved_host
            if resolved_port:
                netloc = f"{resolved_host}:{resolved_port}"

            if resolved_path and not resolved_path.startswith('/'):
                resolved_path = '/' + resolved_path

            query_str = ''
            if query is not None:
                resolved_query = context.resolve_value(query) if context else query
                if isinstance(resolved_query, dict):
                    query_str = urlencode(resolved_query)
                else:
                    query_str = str(resolved_query)

            result_url = urlunparse((resolved_scheme, netloc, resolved_path, '', query_str, resolved_fragment))

            if context:
                context.set(output_var, result_url)
            return ActionResult(success=True, message=result_url, data={'url': result_url})
        except Exception as e:
            return ActionResult(success=False, message=f"URL build error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'scheme': 'https', 'path': '', 'query': None, 'fragment': '', 'port': None, 'output_var': 'url_built'}


class UrlJoinAction(BaseAction):
    """Join URL with path."""
    action_type = "url_join"
    display_name = "URL拼接"
    description = "拼接URL路径"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL join."""
        base_url = params.get('base_url', '')
        path = params.get('path', '')
        output_var = params.get('output_var', 'url_joined')

        if not base_url:
            return ActionResult(success=False, message="base_url is required")

        try:
            resolved_base = context.resolve_value(base_url) if context else base_url
            resolved_path = context.resolve_value(path) if context else path

            result = urljoin(resolved_base, resolved_path)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=result, data={'url': result})
        except Exception as e:
            return ActionResult(success=False, message=f"URL join error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['base_url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '', 'output_var': 'url_joined'}


class UrlEncodeAction(BaseAction):
    """URL encode string."""
    action_type = "url_encode"
    display_name = "URL编码"
    description = "URL编码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL encode."""
        value = params.get('value', '')
        safe = params.get('safe', '')
        output_var = params.get('output_var', 'url_encoded')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            resolved_safe = context.resolve_value(safe) if context else safe

            result = quote(str(resolved), safe=resolved_safe)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=result, data={'encoded': result})
        except Exception as e:
            return ActionResult(success=False, message=f"URL encode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'safe': '', 'output_var': 'url_encoded'}


class UrlDecodeAction(BaseAction):
    """URL decode string."""
    action_type = "url_decode"
    display_name = "URL解码"
    description = "URL解码"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL decode."""
        value = params.get('value', '')
        output_var = params.get('output_var', 'url_decoded')

        if not value:
            return ActionResult(success=False, message="value is required")

        try:
            resolved = context.resolve_value(value) if context else value
            result = unquote(str(resolved))

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=result[:100], data={'decoded': result})
        except Exception as e:
            return ActionResult(success=False, message=f"URL decode error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_decoded'}


class UrlValidateAction(BaseAction):
    """Validate URL."""
    action_type = "url_validate"
    display_name = "URL验证"
    description = "验证URL"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL validate."""
        url = params.get('url', '')
        schemes = params.get('schemes', ['http', 'https'])
        output_var = params.get('output_var', 'url_valid')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            resolved = context.resolve_value(url) if context else url
            resolved_schemes = context.resolve_value(schemes) if context else schemes

            parsed = urlparse(resolved)

            is_valid = bool(parsed.scheme and parsed.netloc and parsed.scheme in resolved_schemes)

            result = {'valid': is_valid, 'scheme': parsed.scheme, 'netloc': parsed.netloc, 'schemes': resolved_schemes}
            if context:
                context.set(output_var, is_valid)
            return ActionResult(success=is_valid, message=f"URL valid: {is_valid}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"URL validate error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'schemes': ['http', 'https'], 'output_var': 'url_valid'}


class UrlQueryParamsAction(BaseAction):
    """Parse URL query parameters."""
    action_type = "url_query_params"
    display_name = "URL查询参数"
    description = "解析URL查询参数"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute URL query params."""
        url = params.get('url', '')
        output_var = params.get('output_var', 'query_params')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            resolved = context.resolve_value(url) if context else url
            parsed = urlparse(resolved)
            params = parse_qs(parsed.query, keep_blank_values=True)
            # Convert single-element lists to scalars
            result = {k: v[0] if len(v) == 1 else v for k, v in params.items()}

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Found {len(result)} query params", data={'params': result})
        except Exception as e:
            return ActionResult(success=False, message=f"URL query params error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'query_params'}
