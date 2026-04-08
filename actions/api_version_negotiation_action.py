"""API version negotiation action module for RabAI AutoClick.

Provides API versioning strategies: header-based, URL path-based,
query parameter-based, and content negotiation versioning.
"""

from __future__ import annotations

import re
import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class VersionInfo:
    """API version information."""
    version: str
    status: str  # current, deprecated, sunset, retired
    sunset_date: Optional[str] = None
    deprecation_notice: Optional[str] = None
    capabilities: List[str] = field(default_factory=list)
    response_format: str = "json"


@dataclass
class NegotiationResult:
    """Version negotiation outcome."""
    selected_version: str
    content_type: str
    deprecation_warnings: List[str]
    alternative_versions: List[str]


class HeaderVersionNegotiationAction(BaseAction):
    """Handle API versioning via Accept header with vendor MIME type.
    
    Matches Accept header like: application/vnd.app.v2+json
    Supports version ranges and fallback to oldest supported.
    
    Args:
        supported_versions: List of supported version strings (e.g., ["v1","v2"])
    """

    def __init__(self, supported_versions: Optional[List[str]] = None):
        super().__init__()
        self.supported_versions = supported_versions or ["v1", "v2"]
        self._version_info: Dict[str, VersionInfo] = {
            v: VersionInfo(version=v, status="current", capabilities=[]) 
            for v in self.supported_versions
        }

    def execute(
        self,
        action: str,
        accept_header: Optional[str] = None,
        api_version: Optional[str] = None,
        register_versions: Optional[List[Dict[str, Any]]] = None
    ) -> ActionResult:
        try:
            if action == "register":
                if not register_versions:
                    return ActionResult(success=False, error="register_versions required")
                for reg in register_versions:
                    v = VersionInfo(
                        version=reg["version"],
                        status=reg.get("status", "current"),
                        sunset_date=reg.get("sunset_date"),
                        deprecation_notice=reg.get("deprecation_notice"),
                        capabilities=reg.get("capabilities", []),
                        response_format=reg.get("response_format", "json")
                    )
                    self._version_info[v.version] = v
                    if v.version not in self.supported_versions:
                        self.supported_versions.append(v.version)
                return ActionResult(success=True, data={
                    "registered": len(register_versions),
                    "versions": [v.version for v in self._version_info.values()]
                })

            elif action == "negotiate":
                if not accept_header:
                    # Default to oldest
                    default = sorted(self.supported_versions)[0]
                    return ActionResult(success=True, data={
                        "selected_version": default,
                        "content_type": f"application/vnd.app.{default}+json",
                        "deprecation_warnings": [],
                        "fallback": True
                    })

                # Parse Accept header
                # e.g., application/vnd.app.v2+json, application/vnd.app.v1+json;q=0.9
                pattern = r'application/vnd\.app\.([\w\d\.]+)\+json(?:;q=([\d\.]+))?'
                matches = re.findall(pattern, accept_header)
                if not matches:
                    # Try generic JSON
                    if "application/json" in accept_header:
                        default = sorted(self.supported_versions)[0]
                        return ActionResult(success=True, data={
                            "selected_version": default,
                            "content_type": f"application/vnd.app.{default}+json",
                            "deprecation_warnings": [],
                            "fallback": True
                        })
                    return ActionResult(success=False, error="Could not parse Accept header")

                # Sort by version (prefer newer) then by q-value
                version_q: List[Tuple[str, float]] = []
                for v, q in matches:
                    qval = float(q) if q else 1.0
                    version_q.append((v, qval))

                # Find best match among supported
                sorted_matches = sorted(version_q, key=lambda x: (-x[1], -self._parse_ver(x[0])))
                for v, _ in sorted_matches:
                    if v in self.supported_versions:
                        deprecation_warnings = []
                        if v in self._version_info:
                            info = self._version_info[v]
                            if info.status == "deprecated":
                                deprecation_warnings.append(info.deprecation_notice or f"Version {v} is deprecated")
                            elif info.status == "sunset":
                                deprecation_warnings.append(f"Version {v} sunset date: {info.sunset_date}")
                        return ActionResult(success=True, data={
                            "selected_version": v,
                            "content_type": f"application/vnd.app.{v}+json",
                            "deprecation_warnings": deprecation_warnings,
                            "status": self._version_info.get(v, VersionInfo(version=v, status="current")).status
                        })

                # No match - return oldest
                oldest = sorted(self.supported_versions)[0]
                return ActionResult(success=True, data={
                    "selected_version": oldest,
                    "content_type": f"application/vnd.app.{oldest}+json",
                    "deprecation_warnings": [f"No matching version found, using {oldest}"],
                    "fallback": True
                })

            elif action == "get_versions":
                return ActionResult(success=True, data={
                    "versions": [
                        {"version": v.version, "status": v.status, "sunset_date": v.sunset_date,
                         "capabilities": v.capabilities}
                        for v in self._version_info.values()
                    ]
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def _parse_ver(self, v: str) -> int:
        """Parse version string to int for comparison."""
        parts = re.findall(r'\d+', v)
        return int(parts[0]) if parts else 0


class URLPathVersionNegotiationAction(BaseAction):
    """Handle API versioning via URL path prefix.
    
    Matches /api/v2/resource patterns. Provides version extraction,
    route matching, and deprecation warnings.
    
    Routes:
        /api/v1/... -> v1
        /api/v2/... -> v2
    """

    def execute(
        self,
        action: str,
        url_path: Optional[str] = None,
        supported_versions: Optional[List[str]] = None
    ) -> ActionResult:
        try:
            versions = supported_versions or ["v1", "v2"]
            version_pattern = r'/api/(v\d+)/'
            
            if action == "extract":
                if not url_path:
                    return ActionResult(success=False, error="url_path required")
                
                match = re.search(version_pattern, url_path)
                if match:
                    found_version = match.group(1)
                    if found_version in versions:
                        return ActionResult(success=True, data={
                            "version": found_version,
                            "is_supported": True,
                            "deprecation_warnings": []
                        })
                    else:
                        return ActionResult(success=False, error=f"Version {found_version} not supported")
                else:
                    # Default to first version
                    default = versions[0]
                    return ActionResult(success=True, data={
                        "version": default,
                        "is_supported": True,
                        "deprecation_warnings": [f"No version in path, defaulting to {default}"],
                        "fallback": True
                    })

            elif action == "build_path":
                if not url_path:
                    return ActionResult(success=False, error="url_path required")
                
                # Replace or insert version in path
                new_path = re.sub(version_pattern, f'/api/{versions[-1]}/', url_path)
                if new_path == url_path and '/api/' in url_path:
                    new_path = re.sub(r'/api/[^/]+', f'/api/{versions[-1]}', url_path)
                
                return ActionResult(success=True, data={
                    "original_path": url_path,
                    "new_path": new_path,
                    "version": versions[-1]
                })

            elif action == "list_versions":
                return ActionResult(success=True, data={
                    "supported_versions": versions,
                    "latest": versions[-1],
                    "oldest": versions[0]
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class QueryParamVersionNegotiationAction(BaseAction):
    """Handle API versioning via query parameter.
    
    Matches ?version=v2 or ?api_version=2 patterns.
    Simple but not cache-friendly.
    """

    def execute(
        self,
        action: str,
        query_string: Optional[str] = None,
        supported_versions: Optional[List[str]] = None,
        default_version: Optional[str] = None
    ) -> ActionResult:
        try:
            versions = supported_versions or ["v1", "v2"]
            default = default_version or versions[0]
            
            if action == "extract":
                if not query_string:
                    return ActionResult(success=True, data={
                        "version": default,
                        "source": "default",
                        "deprecation_warnings": []
                    })

                # Parse query string
                params: Dict[str, str] = {}
                for pair in query_string.split("&"):
                    if "=" in pair:
                        k, v = pair.split("=", 1)
                        params[k] = v

                # Try version param
                found = params.get("version") or params.get("api_version") or params.get("v")
                if found:
                    found = f"v{found}" if not found.startswith("v") else found
                    if found in versions:
                        return ActionResult(success=True, data={
                            "version": found,
                            "source": "query_param",
                            "is_supported": True,
                            "deprecation_warnings": []
                        })
                    else:
                        return ActionResult(success=False, error=f"Unsupported version: {found}")

                return ActionResult(success=True, data={
                    "version": default,
                    "source": "default",
                    "deprecation_warnings": []
                })

            elif action == "build_url":
                if not query_string:
                    qs = f"version={default}"
                else:
                    qs = query_string
                    if "version=" in qs or "api_version=" in qs or "&v=" in qs:
                        qs = re.sub(r'(version|api_version|v)=[^&]+', f'version={default}', qs)
                    else:
                        qs = f"version={default}&{qs}"
                return ActionResult(success=True, data={
                    "query_string": qs,
                    "version": default
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
