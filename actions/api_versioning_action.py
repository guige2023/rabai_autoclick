"""API versioning action module for RabAI AutoClick.

Provides API versioning management including version detection,
version negotiation, deprecation handling, and migration support.
"""

import re
import time
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiVersionDetectorAction(BaseAction):
    """Detect API version from request headers, URL path, or query params.
    
    Supports Accept-Header versioning (application/vnd.api+json),
    URL path versioning (/v1/, /v2/), and query param versioning.
    """
    action_type = "api_version_detector"
    display_name = "API版本检测"
    description = "从请求中检测API版本"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Detect API version from request.
        
        Args:
            context: Execution context.
            params: Dict with keys: request, strategy (header|path|query),
                   header_name, path_pattern, query_param.
        
        Returns:
            ActionResult with detected version info.
        """
        request = params.get('request', {})
        strategy = params.get('strategy', 'header')
        header_name = params.get('header_name', 'Accept')
        path_pattern = params.get('path_pattern', r'/v(\d+)')
        query_param = params.get('query_param', 'version')
        start_time = time.time()

        version = None
        raw_version = None

        if strategy == 'header':
            raw_version = request.get('headers', {}).get(header_name, '')
            version = self._extract_from_accept_header(raw_version)
        elif strategy == 'path':
            path = request.get('path', '')
            match = re.search(path_pattern, path)
            if match:
                version = match.group(1)
                raw_version = path
        elif strategy == 'query':
            query = request.get('query', request.get('params', {}))
            version = query.get(query_param, '')
            raw_version = version

        if not version:
            return ActionResult(
                success=True,
                message="No version detected, using default",
                data={
                    'version': 'default',
                    'raw_version': raw_version,
                    'strategy': strategy,
                    'is_deprecated': False
                },
                duration=time.time() - start_time
            )

        is_deprecated = self._is_deprecated_version(version)
        return ActionResult(
            success=True,
            message=f"Detected API version: v{version}",
            data={
                'version': version,
                'raw_version': raw_version,
                'strategy': strategy,
                'is_deprecated': is_deprecated
            },
            duration=time.time() - start_time
        )

    def _extract_from_accept_header(self, header: str) -> Optional[str]:
        """Extract version from Accept header."""
        patterns = [
            r'application/vnd\.api\+json;version=(\d+)',
            r'application/vnd\.v(\d+)\+json',
            r'version=(\d+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, header)
            if match:
                return match.group(1)
        return None

    def _is_deprecated_version(self, version: str) -> bool:
        """Check if version is deprecated."""
        deprecated = params.get('deprecated_versions', [])
        return version in deprecated


class ApiVersionNegotiatorAction(BaseAction):
    """Negotiate best API version between client and server.
    
    Compares client's acceptable versions with server's supported
    versions and returns the best match.
    """
    action_type = "api_version_negotiator"
    display_name = "API版本协商"
    description = "客户端与服务器之间协商最佳API版本"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Negotiate API version.
        
        Args:
            context: Execution context.
            params: Dict with keys: client_versions (list),
                   server_versions (list), default_version.
        
        Returns:
            ActionResult with negotiated version.
        """
        client_versions = params.get('client_versions', [])
        server_versions = params.get('server_versions', [])
        default_version = params.get('default_version', '1')
        start_time = time.time()

        if not client_versions:
            return ActionResult(
                success=True,
                message=f"No client versions, using default: v{default_version}",
                data={'version': default_version, 'negotiated': False},
                duration=time.time() - start_time
            )

        client_versions_sorted = sorted(client_versions, key=lambda v: int(v), reverse=True)

        for cv in client_versions_sorted:
            if cv in server_versions:
                return ActionResult(
                    success=True,
                    message=f"Negotiated version: v{cv}",
                    data={'version': cv, 'negotiated': True},
                    duration=time.time() - start_time
                )

        fallback = server_versions[0] if server_versions else default_version
        return ActionResult(
            success=True,
            message=f"No match, falling back to: v{fallback}",
            data={'version': fallback, 'negotiated': False},
            duration=time.time() - start_time
        )


class ApiVersionRouterAction(BaseAction):
    """Route requests to appropriate handler based on API version.
    
    Maps version numbers to handler endpoints or service URLs.
    """
    action_type = "api_version_router"
    display_name = "API版本路由"
    description = "根据API版本路由请求到不同处理器"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Route request based on version.
        
        Args:
            context: Execution context.
            params: Dict with keys: version, version_map (dict),
                   default_handler, strip_version.
        
        Returns:
            ActionResult with routed handler info.
        """
        version = params.get('version', '1')
        version_map = params.get('version_map', {})
        default_handler = params.get('default_handler', '')
        strip_version = params.get('strip_version', True)
        start_time = time.time()

        handler = version_map.get(version)
        if not handler:
            if default_handler:
                handler = default_handler
            else:
                return ActionResult(
                    success=False,
                    message=f"No handler found for version: v{version}",
                    data={'available_versions': list(version_map.keys())}
                )

        return ActionResult(
            success=True,
            message=f"Routed to handler for v{version}",
            data={
                'version': version,
                'handler': handler,
                'strip_version': strip_version
            },
            duration=time.time() - start_time
        )


class ApiDeprecationHandlerAction(BaseAction):
    """Handle deprecated API versions with migration guidance.
    
    Detects deprecated version usage and provides migration
    instructions and timeline.
    """
    action_type = "api_deprecation_handler"
    display_name = "API版本废弃处理"
    description = "处理API版本废弃并提供迁移指导"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Handle deprecated API version.
        
        Args:
            context: Execution context.
            params: Dict with keys: version, deprecated_versions (dict),
                   sunset_date, migration_guide.
        
        Returns:
            ActionResult with deprecation info and recommendations.
        """
        version = params.get('version', '')
        deprecated_versions = params.get('deprecated_versions', {})
        sunset_date = params.get('sunset_date', '')
        migration_guide = params.get('migration_guide', '')
        start_time = time.time()

        if version not in deprecated_versions:
            return ActionResult(
                success=True,
                message=f"Version v{version} is not deprecated",
                data={
                    'is_deprecated': False,
                    'version': version
                },
                duration=time.time() - start_time
            )

        deprecation_info = deprecated_versions[version]
        return ActionResult(
            success=True,
            message=f"Version v{version} is deprecated",
            data={
                'is_deprecated': True,
                'version': version,
                'sunset_date': sunset_date or deprecation_info.get('sunset_date'),
                'migration_guide': migration_guide or deprecation_info.get('migration_guide'),
                'successor_version': deprecation_info.get('successor'),
                'warning_message': deprecation_info.get('warning', f"API v{version} is deprecated")
            },
            duration=time.time() - start_time
        )


class ApiVersionMigratorAction(BaseAction):
    """Migrate request data between API versions.
    
    Applies transformation rules to convert request payload
    from one version format to another.
    """
    action_type = "api_version_migrator"
    display_name = "API版本迁移"
    description = "在不同API版本之间迁移数据格式"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Migrate request data between versions.
        
        Args:
            context: Execution context.
            params: Dict with keys: from_version, to_version,
                   data, migration_rules.
        
        Returns:
            ActionResult with migrated data.
        """
        from_version = params.get('from_version', '')
        to_version = params.get('to_version', '')
        data = params.get('data', {})
        migration_rules = params.get('migration_rules', {})
        start_time = time.time()

        if not from_version or not to_version:
            return ActionResult(
                success=False,
                message="from_version and to_version are required"
            )

        if from_version == to_version:
            return ActionResult(
                success=True,
                message="Same version, no migration needed",
                data={'data': data, 'migrated': False},
                duration=time.time() - start_time
            )

        rule_key = f"{from_version}_to_{to_version}"
        rules = migration_rules.get(rule_key, {})

        migrated_data = self._apply_migration_rules(data, rules)
        return ActionResult(
            success=True,
            message=f"Migrated data from v{from_version} to v{to_version}",
            data={
                'data': migrated_data,
                'migrated': True,
                'from_version': from_version,
                'to_version': to_version,
                'rules_applied': len(rules)
            },
            duration=time.time() - start_time
        )

    def _apply_migration_rules(
        self,
        data: Dict[str, Any],
        rules: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply migration transformation rules to data."""
        result = dict(data)
        renamed = rules.get('rename', {})
        removed = rules.get('remove', [])
        transformed = rules.get('transform', {})

        for old_key, new_key in renamed.items():
            if old_key in result:
                result[new_key] = result.pop(old_key)

        for key in removed:
            result.pop(key, None)

        for key, transformer in transformed.items():
            if key in result:
                result[key] = transformer.get(result[key], result[key])

        return result
