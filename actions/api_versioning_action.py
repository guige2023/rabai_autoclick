"""API versioning action module for RabAI AutoClick.

Provides API versioning support with version negotiation, deprecation
warnings, and backward compatibility handling.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from packaging import version

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class VersionScheme(Enum):
    """API versioning scheme."""
    HEADER = "header"
    PATH = "path"
    QUERY = "query"
    CONTENT_TYPE = "content_type"


@dataclass
class APIVersion:
    """An API version definition."""
    version: str
    status: str = "current"  # current, deprecated, sunset
    sunset_date: Optional[str] = None
    description: str = ""
    handlers: Dict[str, Callable] = field(default_factory=dict)


@dataclass
class VersionInfo:
    """Information about a resolved API version."""
    version: str
    handler: Callable
    status: str
    is_sunset: bool
    deprecation_warning: Optional[str] = None


class APIVersioningAction(BaseAction):
    """Handle API versioning with negotiation and deprecation.
    
    Supports header-based, path-based, and query-based versioning
    with automatic version resolution and deprecation warnings.
    """
    action_type = "api_versioning"
    display_name = "API版本控制"
    description = "API版本协商和废弃管理"
    
    def __init__(self):
        super().__init__()
        self._versions: Dict[str, APIVersion] = {}
        self._default_version: Optional[str] = None
        self._scheme = VersionScheme.HEADER
    
    def register_version(
        self,
        version_str: str,
        handler: Callable,
        status: str = "current",
        sunset_date: Optional[str] = None,
        description: str = ""
    ) -> None:
        """Register an API version with its handler."""
        ver = APIVersion(
            version=version_str,
            status=status,
            sunset_date=sunset_date,
            description=description,
            handlers={'_default': handler}
        )
        self._versions[version_str] = ver
        if self._default_version is None:
            self._default_version = version_str
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute API versioning operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'resolve', 'negotiate', 'register', 'deprecate'
                - version: Version string (for register/deprecate)
                - request: Request dict (for resolve/negotiate)
                - supported: List of supported versions (for negotiate)
        
        Returns:
            ActionResult with version resolution result.
        """
        operation = params.get('operation', 'resolve').lower()
        
        if operation == 'resolve':
            return self._resolve(params)
        elif operation == 'negotiate':
            return self._negotiate(params)
        elif operation == 'register':
            return self._register(params)
        elif operation == 'deprecate':
            return self._deprecate(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _resolve(self, params: Dict[str, Any]) -> ActionResult:
        """Resolve version from request."""
        request = params.get('request', {})
        scheme = params.get('scheme', 'header').lower()
        
        version_str = None
        
        if scheme == 'header':
            version_str = request.get('headers', {}).get('API-Version') or \
                         request.get('headers', {}).get('Accept')
        elif scheme == 'path':
            path = request.get('path', '')
            # Extract version from path like /v1/resource
            import re
            match = re.search(r'/v(\d+(?:\.\d+)?)/', path)
            if match:
                version_str = f"v{match.group(1)}"
        elif scheme == 'query':
            version_str = request.get('query', {}).get('version')
        
        if not version_str and self._default_version:
            version_str = self._default_version
        
        if version_str not in self._versions:
            return ActionResult(
                success=False,
                message=f"Unknown version: {version_str}",
                data={
                    'available_versions': list(self._versions.keys())
                }
            )
        
        ver = self._versions[version_str]
        handler = ver.handlers.get('_default')
        
        deprecation_warning = None
        if ver.status == 'deprecated':
            deprecation_warning = f"API version {version_str} is deprecated"
            if ver.sunset_date:
                deprecation_warning += f". Will be sunset on {ver.sunset_date}"
        
        return ActionResult(
            success=True,
            message=f"Resolved version {version_str}",
            data={
                'version': version_str,
                'status': ver.status,
                'handler': handler,
                'deprecation_warning': deprecation_warning
            }
        )
    
    def _negotiate(self, params: Dict[str, Any]) -> ActionResult:
        """Negotiate best version between client and server."""
        request = params.get('request', {})
        supported = params.get('supported', [])
        
        # Get client preferred versions
        client_versions = []
        accept_header = request.get('headers', {}).get('Accept', '')
        if accept_header:
            import re
            client_versions = re.findall(r'v(\d+(?:\.\d+)?)', accept_header)
        
        if not client_versions:
            # Default to first supported
            return ActionResult(
                success=True,
                message="No client preference, using default",
                data={'version': supported[0] if supported else None}
            )
        
        # Find best match
        best_match = None
        for client_ver in client_versions:
            for sup_ver in supported:
                ver_num = sup_ver.lstrip('v')
                if ver_num == client_ver:
                    best_match = sup_ver
                    break
            if best_match:
                break
        
        if not best_match and supported:
            best_match = supported[-1]  # Fall back to oldest supported
        
        return ActionResult(
            success=True,
            message=f"Negotiated version {best_match}",
            data={'version': best_match}
        )
    
    def _register(self, params: Dict[str, Any]) -> ActionResult:
        """Register a new API version."""
        version_str = params.get('version')
        handler = params.get('handler')
        status = params.get('status', 'current')
        sunset_date = params.get('sunset_date')
        description = params.get('description', '')
        
        if not version_str:
            return ActionResult(success=False, message="version is required")
        
        self.register_version(
            version_str=version_str,
            handler=handler,
            status=status,
            sunset_date=sunset_date,
            description=description
        )
        
        return ActionResult(
            success=True,
            message=f"Registered version {version_str}",
            data={'version': version_str}
        )
    
    def _deprecate(self, params: Dict[str, Any]) -> ActionResult:
        """Mark a version as deprecated."""
        version_str = params.get('version')
        sunset_date = params.get('sunset_date')
        message = params.get('message', '')
        
        if not version_str or version_str not in self._versions:
            return ActionResult(
                success=False,
                message=f"Unknown version: {version_str}"
            )
        
        ver = self._versions[version_str]
        ver.status = 'deprecated'
        if sunset_date:
            ver.sunset_date = sunset_date
        
        return ActionResult(
            success=True,
            message=f"Deprecated version {version_str}",
            data={'version': version_str, 'sunset_date': sunset_date}
        )


class VersionCompatAction(BaseAction):
    """Handle backward compatibility across API versions."""
    action_type = "version_compat"
    display_name = "版本兼容"
    description = "处理跨版本兼容性转换"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute version compatibility handling.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'adapt_request', 'adapt_response', 'check_compat'
                - version_from: Source version
                - version_to: Target version
                - data: Request/response data
                - compat_rules: Compatibility transformation rules
        
        Returns:
            ActionResult with adapted data.
        """
        operation = params.get('operation', 'adapt_request').lower()
        
        if operation == 'adapt_request':
            return self._adapt_request(params)
        elif operation == 'adapt_response':
            return self._adapt_response(params)
        elif operation == 'check_compat':
            return self._check_compat(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _adapt_request(self, params: Dict[str, Any]) -> ActionResult:
        """Adapt request data to target version."""
        version_from = params.get('version_from')
        version_to = params.get('version_to')
        data = params.get('data', {})
        compat_rules = params.get('compat_rules', {})
        
        if not version_from or not version_to:
            return ActionResult(
                success=False,
                message="version_from and version_to required"
            )
        
        # Apply compatibility transformations
        adapted = dict(data)
        compat_key = f"{version_from}_to_{version_to}"
        
        if compat_key in compat_rules:
            rules = compat_rules[compat_key]
            adapted = self._apply_compat_rules(adapted, rules)
        
        return ActionResult(
            success=True,
            message=f"Adapted request from {version_from} to {version_to}",
            data={'adapted': adapted}
        )
    
    def _adapt_response(self, params: Dict[str, Any]) -> ActionResult:
        """Adapt response data from source version."""
        version_from = params.get('version_from')
        version_to = params.get('version_to')
        data = params.get('data', {})
        compat_rules = params.get('compat_rules', {})
        
        adapted = dict(data)
        compat_key = f"{version_from}_to_{version_to}"
        
        if compat_key in compat_rules:
            rules = compat_rules[compat_key]
            adapted = self._apply_compat_rules(adapted, rules)
        
        return ActionResult(
            success=True,
            message=f"Adapted response",
            data={'adapted': adapted}
        )
    
    def _check_compat(self, params: Dict[str, Any]) -> ActionResult:
        """Check compatibility between versions."""
        version_from = params.get('version_from')
        version_to = params.get('version_to')
        compat_rules = params.get('compat_rules', {})
        
        compat_key = f"{version_from}_to_{version_to}"
        has_rules = compat_key in compat_rules
        
        return ActionResult(
            success=True,
            message=f"Compatibility rules {'found' if has_rules else 'not found'}",
            data={
                'compatible': has_rules,
                'compat_key': compat_key
            }
        )
    
    def _apply_compat_rules(
        self,
        data: Dict,
        rules: List[Dict]
    ) -> Dict:
        """Apply compatibility transformation rules."""
        result = dict(data)
        
        for rule in rules:
            op = rule.get('op')
            path = rule.get('path')
            value = rule.get('value')
            
            if op == 'rename':
                old_name = rule.get('from')
                if old_name in result:
                    result[path] = result.pop(old_name)
            elif op == 'remove':
                if path in result:
                    del result[path]
            elif op == 'add':
                result[path] = value
            elif op == 'transform':
                if path in result:
                    result[path] = value(result[path])
        
        return result
