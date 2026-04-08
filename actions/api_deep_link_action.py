"""API Deep Link action module for RabAI AutoClick.

Handles deep link URL parsing, parameter extraction,
and navigation flows.
"""

import json
import time
import sys
import os
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs, urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiDeepLinkParserAction(BaseAction):
    """Parse deep link URLs and extract structured data.

    Handles custom URL schemes, path parameters,
    and query string parsing.
    """
    action_type = "api_deep_link_parser"
    display_name = "深度链接解析器"
    description = "解析深度链接URL并提取结构化数据"

    SCHEME_PATTERNS = {
        'custom': r'^([a-zA-Z][a-zA-Z0-9+-.]*):',
        'app': r'^(?:myapp|app)://',
        'deeplink': r'^(?:https?://)?(?:www\.)?[\w.-]+(?:/[\w.-]*)*',
    }

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse a deep link URL.

        Args:
            context: Execution context.
            params: Dict with keys: url, expected_schemes,
                   extract_params, normalize.

        Returns:
            ActionResult with parsed deep link data.
        """
        start_time = time.time()
        try:
            url = params.get('url', '')
            expected_schemes = params.get('expected_schemes', [])
            extract_params = params.get('extract_params', True)
            normalize = params.get('normalize', True)

            if not url:
                return ActionResult(
                    success=False,
                    message="URL is required",
                    duration=time.time() - start_time,
                )

            parsed = urlparse(url)

            # Detect scheme
            scheme = parsed.scheme.lower() if parsed.scheme else 'unknown'
            is_valid = True
            if expected_schemes and scheme not in expected_schemes:
                is_valid = False

            # Extract path segments
            path = parsed.path.strip('/')
            path_segments = [s for s in path.split('/') if s]

            # Extract query parameters
            query_params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}

            # Extract fragment
            fragment = parsed.fragment

            # Build normalized URL
            normalized_url = url
            if normalize:
                parts = []
                if scheme:
                    parts.append(f"{scheme}://")
                if parsed.netloc:
                    parts.append(parsed.netloc)
                if path:
                    parts.append('/' + path)
                if query_params:
                    parts.append('?' + urlencode({k: v if not isinstance(v, list) else v[0] for k, v in query_params.items()}))
                normalized_url = ''.join(parts)

            result = {
                'original_url': url,
                'scheme': scheme,
                'host': parsed.netloc,
                'path': path,
                'path_segments': path_segments,
                'query_params': query_params,
                'fragment': fragment,
                'is_valid': is_valid,
                'normalized_url': normalized_url,
            }

            duration = time.time() - start_time
            return ActionResult(
                success=is_valid,
                message=f"Parsed deep link: {scheme}://{parsed.netloc}/{path}",
                data=result,
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Deep link parse error: {str(e)}",
                duration=duration,
            )


class ApiDeepLinkBuilderAction(BaseAction):
    """Build deep link URLs from parameters.

    Constructs URLs for various platforms with
    fallback handling.
    """
    action_type = "api_deep_link_builder"
    display_name = "深度链接构建器"
    description = "从参数构建深度链接URL"

    PLATFORM_DEFAULTS = {
        'ios': {'scheme': 'myapp', 'path_prefix': '/'},
        'android': {'scheme': 'myapp', 'path_prefix': '/'},
        'web': {'scheme': 'https', 'path_prefix': '/'},
        'universal': {'scheme': 'https', 'path_prefix': '/'},
    }

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Build deep link URLs.

        Args:
            context: Execution context.
            params: Dict with keys: platform, base_url, path_segments,
                   query_params, fallback_url.

        Returns:
            ActionResult with built URLs.
        """
        start_time = time.time()
        try:
            platform = params.get('platform', 'universal')
            base_url = params.get('base_url', '')
            path_segments = params.get('path_segments', [])
            query_params = params.get('query_params', {})
            fallback_url = params.get('fallback_url', '')

            defaults = self.PLATFORM_DEFAULTS.get(platform, self.PLATFORM_DEFAULTS['universal'])
            scheme = params.get('scheme', defaults['scheme'])
            path_prefix = params.get('path_prefix', defaults['path_prefix'])

            # Build path
            path = path_prefix.strip('/')
            if path_segments:
                path += '/' + '/'.join(str(s) for s in path_segments)

            # Build URL
            if base_url:
                full_url = base_url.rstrip('/') + '/' + path.lstrip('/')
            else:
                full_url = f"{scheme}://{path}"

            # Add query params
            if query_params:
                qs = urlencode({k: str(v) if not isinstance(v, list) else v[0] for k, v in query_params.items()})
                full_url = full_url + ('?' if '?' not in full_url else '&') + qs

            result = {
                'platform': platform,
                'url': full_url,
                'fallback_url': fallback_url,
                'is_universal': platform == 'universal',
            }

            # Add app link alternatives
            if platform in ('ios', 'android'):
                result['alternative_urls'] = [
                    full_url,
                    fallback_url or full_url,
                ]

            duration = time.time() - start_time
            return ActionResult(
                success=True,
                message=f"Built {platform} deep link: {full_url}",
                data=result,
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Deep link builder error: {str(e)}",
                duration=duration,
            )


class ApiDeepLinkRouterAction(BaseAction):
    """Route deep links to appropriate handlers.

    Matches URL patterns to handler functions
    and extracts route parameters.
    """
    action_type = "api_deep_link_router"
    display_name = "深度链接路由器"
    description = "将深度链接路由到对应处理器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route a deep link.

        Args:
            context: Execution context.
            params: Dict with keys: url, routes (list of route defs).

        Returns:
            ActionResult with matched route and params.
        """
        start_time = time.time()
        try:
            url = params.get('url', '')
            routes = params.get('routes', [])

            if not url:
                return ActionResult(
                    success=False,
                    message="URL is required",
                    duration=time.time() - start_time,
                )

            if not routes:
                return ActionResult(
                    success=False,
                    message="No routes defined",
                    duration=time.time() - start_time,
                )

            matched_route = None
            route_params = {}
            for route in routes:
                pattern = route.get('pattern', '')
                path = urlparse(url).path

                # Convert route pattern to regex
                regex_pattern = pattern.replace('{', '(?P<').replace('}', '>[^/]+)')
                regex_pattern = '^' + regex_pattern + '$'

                match = re.match(regex_pattern, path)
                if match:
                    matched_route = route
                    route_params = match.groupdict()
                    break

            if matched_route:
                handler = matched_route.get('handler')
                if callable(handler):
                    try:
                        handler_result = handler({'url': url, 'params': route_params}, context)
                        return ActionResult(
                            success=True,
                            message=f"Routed to {matched_route.get('name', 'handler')}",
                            data={
                                'route': matched_route.get('name'),
                                'params': route_params,
                                'handler_result': handler_result,
                            },
                            duration=time.time() - start_time,
                        )
                    except Exception as e:
                        return ActionResult(
                            success=False,
                            message=f"Handler error: {str(e)}",
                            data={'route': matched_route.get('name'), 'params': route_params},
                            duration=time.time() - start_time,
                        )
                else:
                    return ActionResult(
                        success=True,
                        message=f"Matched route: {matched_route.get('name', 'unknown')}",
                        data={'route': matched_route, 'params': route_params},
                        duration=time.time() - start_time,
                    )

            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message="No matching route found",
                data={'url': url, 'available_routes': [r.get('pattern') for r in routes]},
                duration=duration,
            )

        except Exception as e:
            duration = time.time() - start_time
            return ActionResult(
                success=False,
                message=f"Router error: {str(e)}",
                duration=duration,
            )
