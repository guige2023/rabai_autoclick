"""Url action module for RabAI AutoClick.

Provides URL operations:
- UrlParseAction: Parse URL
- UrlBuildAction: Build URL
- UrlEncodeAction: URL encode
- UrlDecodeAction: URL decode
- UrlValidateAction: Validate URL
"""

from urllib.parse import urlparse, urlencode, quote, unquote
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class UrlParseAction(BaseAction):
    """Parse URL."""
    action_type = "url_parse"
    display_name = "URL解析"
    description = "解析URL各部分"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL parse.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with parsed URL.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'parsed_url')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved = context.resolve_value(url)
            parsed = urlparse(resolved)

            result = {
                'scheme': parsed.scheme,
                'netloc': parsed.netloc,
                'path': parsed.path,
                'params': parsed.params,
                'query': parsed.query,
                'fragment': parsed.fragment
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL解析完成",
                data={
                    'url': resolved,
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
        return {'output_var': 'parsed_url'}


class UrlBuildAction(BaseAction):
    """Build URL."""
    action_type = "url_build"
    display_name = "URL构建"
    description = "构建完整URL"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL build.

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
        output_var = params.get('output_var', 'built_url')

        try:
            resolved_scheme = context.resolve_value(scheme)
            resolved_host = context.resolve_value(host)
            resolved_path = context.resolve_value(path)
            resolved_query = context.resolve_value(query)

            if resolved_query and isinstance(resolved_query, dict):
                query_str = urlencode(resolved_query)
            elif resolved_query:
                query_str = resolved_query
            else:
                query_str = ''

            if resolved_path and not resolved_path.startswith('/'):
                resolved_path = '/' + resolved_path

            result = f"{resolved_scheme}://{resolved_host}{resolved_path}"
            if query_str:
                result += '?' + query_str

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
        return ['host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'scheme': 'https', 'path': '', 'query': {}, 'output_var': 'built_url'}


class UrlEncodeAction(BaseAction):
    """URL encode."""
    action_type = "url_encode"
    display_name = "URL编码"
    description = "对字符串进行URL编码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL encode.

        Args:
            context: Execution context.
            params: Dict with text, safe, output_var.

        Returns:
            ActionResult with encoded URL.
        """
        text = params.get('text', '')
        safe = params.get('safe', '')
        output_var = params.get('output_var', 'encoded_url')

        try:
            resolved = context.resolve_value(text)
            resolved_safe = context.resolve_value(safe) if safe else ''

            result = quote(resolved, safe=resolved_safe)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL编码完成",
                data={
                    'original': resolved,
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
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'safe': '', 'output_var': 'encoded_url'}


class UrlDecodeAction(BaseAction):
    """URL decode."""
    action_type = "url_decode"
    display_name = "URL解码"
    description = "对URL编码字符串进行解码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL decode.

        Args:
            context: Execution context.
            params: Dict with text, output_var.

        Returns:
            ActionResult with decoded string.
        """
        text = params.get('text', '')
        output_var = params.get('output_var', 'decoded_url')

        try:
            resolved = context.resolve_value(text)
            result = unquote(resolved)

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL解码完成",
                data={
                    'original': resolved,
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
        return ['text']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'decoded_url'}


class UrlValidateAction(BaseAction):
    """Validate URL."""
    action_type = "url_validate"
    display_name = "URL验证"
    description = "验证URL格式是否正确"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute URL validate.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with validation result.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'valid_result')

        try:
            resolved = context.resolve_value(url)
            parsed = urlparse(resolved)

            result = all([
                parsed.scheme in ('http', 'https', 'ftp', 'ftps'),
                parsed.netloc,
                len(resolved) < 2048
            ])

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL验证: {'有效' if result else '无效'}",
                data={
                    'url': resolved,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"URL验证失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'valid_result'}