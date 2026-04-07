"""Encode action module for RabAI AutoClick.

Provides URL encoding/decoding operations:
- UrlEncodeAction: URL encode string
- UrlDecodeAction: URL decode string
- UrlQuoteAction: Quote URL string
- UrlUnquoteAction: Unquote URL string
"""

import urllib.parse
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UrlEncodeAction(BaseAction):
    """URL encode string."""
    action_type = "url_encode"
    display_name = "URL编码"
    description = "对字符串进行URL编码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL encoding.

        Args:
            context: Execution context.
            params: Dict with value, safe, output_var.

        Returns:
            ActionResult with encoded string.
        """
        value = params.get('value', '')
        safe = params.get('safe', '')
        output_var = params.get('output_var', 'url_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_safe = context.resolve_value(safe) if safe else ''

            result = urllib.parse.quote_plus(resolved, safe=resolved_safe)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL编码完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'safe': '', 'output_var': 'url_result'}


class UrlDecodeAction(BaseAction):
    """URL decode string."""
    action_type = "url_decode"
    display_name = "URL解码"
    description = "对字符串进行URL解码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL decoding.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with decoded string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'url_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = urllib.parse.unquote_plus(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL解码完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_result'}


class UrlQuoteAction(BaseAction):
    """Quote URL string."""
    action_type = "url_quote"
    display_name = "URL引号编码"
    description = "对字符串进行URL引号编码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL quoting.

        Args:
            context: Execution context.
            params: Dict with value, safe, output_var.

        Returns:
            ActionResult with quoted string.
        """
        value = params.get('value', '')
        safe = params.get('safe', '/')
        output_var = params.get('output_var', 'url_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            resolved_safe = context.resolve_value(safe) if safe else ''

            result = urllib.parse.quote(resolved, safe=resolved_safe)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL引号编码完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL引号编码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'safe': '/', 'output_var': 'url_result'}


class UrlUnquoteAction(BaseAction):
    """Unquote URL string."""
    action_type = "url_unquote"
    display_name = "URL引号解码"
    description = "对字符串进行URL引号解码"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL unquoting.

        Args:
            context: Execution context.
            params: Dict with value, output_var.

        Returns:
            ActionResult with unquoted string.
        """
        value = params.get('value', '')
        output_var = params.get('output_var', 'url_result')

        valid, msg = self.validate_type(value, str, 'value')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(value)
            result = urllib.parse.unquote(resolved)
            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL引号解码完成",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL引号解码失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['value']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_result'}


class UrlParseAction(BaseAction):
    """Parse URL."""
    action_type = "url_parse"
    display_name = "解析URL"
    description = "解析URL字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL parsing.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with parsed URL components.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'url_result')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(url)
            parsed = urllib.parse.urlparse(resolved)

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

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL解析完成: {parsed.scheme}://{parsed.hostname}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL解析失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_result'}


class UrlBuildAction(BaseAction):
    """Build URL."""
    action_type = "url_build"
    display_name = "构建URL"
    description = "构建URL字符串"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL building.

        Args:
            context: Execution context.
            params: Dict with scheme, host, path, query, output_var.

        Returns:
            ActionResult with built URL.
        """
        scheme = params.get('scheme', 'https')
        host = params.get('host', '')
        path = params.get('path', '')
        query = params.get('query', {})
        output_var = params.get('output_var', 'url_result')

        valid, msg = self.validate_type(scheme, str, 'scheme')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_scheme = context.resolve_value(scheme)
            resolved_host = context.resolve_value(host)
            resolved_path = context.resolve_value(path) if path else ''
            resolved_query = context.resolve_value(query) if query else {}

            if resolved_path and not resolved_path.startswith('/'):
                resolved_path = '/' + resolved_path

            netloc = resolved_host

            query_string = ''
            if resolved_query:
                if isinstance(resolved_query, dict):
                    query_string = urllib.parse.urlencode(resolved_query)
                elif isinstance(resolved_query, str):
                    query_string = resolved_query

            result = urllib.parse.urlunparse((
                resolved_scheme,
                netloc,
                resolved_path,
                '',
                query_string,
                ''
            ))

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL构建完成: {result}",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL构建失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['scheme', 'host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '', 'query': {}, 'output_var': 'url_result'}