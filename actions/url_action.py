"""URL action module for RabAI AutoClick.

Provides URL operations:
- UrlParseAction: Parse URL
- UrlBuildAction: Build URL
- UrlEncodeAction: URL encode
- UrlDecodeAction: URL decode
- UrlValidateAction: Validate URL
- UrlShortenAction: Shorten URL (simulated)
"""

import urllib.parse
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
        """Execute parse.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with URL components.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'url_parsed')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)

            parsed = urllib.parse.urlparse(resolved_url)

            result = {
                'scheme': parsed.scheme,
                'netloc': parsed.netloc,
                'hostname': parsed.hostname,
                'port': parsed.port,
                'path': parsed.path,
                'params': parsed.params,
                'query': parsed.query,
                'fragment': parsed.fragment,
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"URL已解析: {parsed.scheme}://{parsed.hostname}",
                data={'url': resolved_url, 'parsed': result, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"URL解析失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_parsed'}


class UrlBuildAction(BaseAction):
    """Build URL."""
    action_type = "url_build"
    display_name = "URL构建"
    description = "构建URL"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute build.

        Args:
            context: Execution context.
            params: Dict with scheme, host, path, query, output_var.

        Returns:
            ActionResult with built URL.
        """
        scheme = params.get('scheme', 'https')
        host = params.get('host', '')
        path = params.get('path', '/')
        query = params.get('query', {})
        output_var = params.get('output_var', 'url_built')

        valid, msg = self.validate_type(host, str, 'host')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_scheme = context.resolve_value(scheme)
            resolved_host = context.resolve_value(host)
            resolved_path = context.resolve_value(path)
            resolved_query = context.resolve_value(query) if query else {}

            query_str = urllib.parse.urlencode(resolved_query) if resolved_query else ''

            if resolved_path and not resolved_path.startswith('/'):
                resolved_path = '/' + resolved_path

            if query_str:
                url = f"{resolved_scheme}://{resolved_host}{resolved_path}?{query_str}"
            else:
                url = f"{resolved_scheme}://{resolved_host}{resolved_path}"

            context.set(output_var, url)

            return ActionResult(
                success=True,
                message=f"URL已构建: {url}",
                data={'url': url, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"URL构建失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['scheme', 'host']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'path': '/', 'query': {}, 'output_var': 'url_built'}


class UrlEncodeAction(BaseAction):
    """URL encode."""
    action_type = "url_encode"
    display_name = "URL编码"
    description = "URL编码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute encode.

        Args:
            context: Execution context.
            params: Dict with data, safe, output_var.

        Returns:
            ActionResult with encoded URL.
        """
        data = params.get('data', '')
        safe = params.get('safe', '')
        output_var = params.get('output_var', 'url_encoded')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)
            resolved_safe = context.resolve_value(safe) if safe else ''

            encoded = urllib.parse.quote(resolved_data, safe=resolved_safe)
            context.set(output_var, encoded)

            return ActionResult(
                success=True,
                message=f"URL编码完成",
                data={'encoded': encoded, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"URL编码失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'safe': '', 'output_var': 'url_encoded'}


class UrlDecodeAction(BaseAction):
    """URL decode."""
    action_type = "url_decode"
    display_name = "URL解码"
    description = "URL解码"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute decode.

        Args:
            context: Execution context.
            params: Dict with data, output_var.

        Returns:
            ActionResult with decoded URL.
        """
        data = params.get('data', '')
        output_var = params.get('output_var', 'url_decoded')

        valid, msg = self.validate_type(data, str, 'data')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_data = context.resolve_value(data)

            decoded = urllib.parse.unquote(resolved_data)
            context.set(output_var, decoded)

            return ActionResult(
                success=True,
                message=f"URL解码完成",
                data={'decoded': decoded, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"URL解码失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_decoded'}


class UrlValidateAction(BaseAction):
    """Validate URL."""
    action_type = "url_validate"
    display_name = "URL验证"
    description = "验证URL格式"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute validate.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with validation result.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'url_valid')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)

            parsed = urllib.parse.urlparse(resolved_url)
            is_valid = bool(parsed.scheme and parsed.netloc)

            context.set(output_var, is_valid)

            return ActionResult(
                success=True,
                message=f"URL {'有效' if is_valid else '无效'}",
                data={'url': resolved_url, 'valid': is_valid, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"URL验证失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_valid'}


class UrlQueryParamsAction(BaseAction):
    """Get URL query parameters."""
    action_type = "url_query_params"
    display_name = "URL查询参数"
    description = "提取URL查询参数"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute query params.

        Args:
            context: Execution context.
            params: Dict with url, output_var.

        Returns:
            ActionResult with query params.
        """
        url = params.get('url', '')
        output_var = params.get('output_var', 'url_params')

        valid, msg = self.validate_type(url, str, 'url')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            resolved_url = context.resolve_value(url)

            parsed = urllib.parse.urlparse(resolved_url)
            params_dict = dict(urllib.parse.parse_qsl(parsed.query))

            context.set(output_var, params_dict)

            return ActionResult(
                success=True,
                message=f"查询参数: {len(params_dict)} 个",
                data={'params': params_dict, 'count': len(params_dict), 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"URL查询参数提取失败: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'url_params'}
