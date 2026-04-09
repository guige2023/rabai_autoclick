"""
API Versioning Action Module

Handles API version negotiation, deprecation cycles, and migration support.
Semantic versioning, header-based routing, and backward compatibility.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class VersionScheme(Enum):
    """API version naming schemes."""
    
    SEMVER = "semver"
    DATE = "date"
    SIMPLE = "simple"


@dataclass
class APIVersion:
    """Represents an API version."""
    
    major: int
    minor: int = 0
    patch: int = 0
    raw: str = ""
    
    def __post_init__(self):
        if not self.raw:
            self.raw = f"v{self.major}.{self.minor}.{self.patch}"
    
    @classmethod
    def parse(cls, version_str: str) -> "APIVersion":
        """Parse version from string."""
        version_str = version_str.strip().lstrip("v")
        
        match = re.match(r"(\d+)(?:\.(\d+))?(?:\.(\d+))?", version_str)
        if not match:
            return cls(major=1)
        
        major = int(match.group(1))
        minor = int(match.group(2)) if match.group(2) else 0
        patch = int(match.group(3)) if match.group(3) else 0
        return cls(major=major, minor=minor, patch=patch, raw=f"v{major}.{minor}.{patch}")
    
    def is_compatible(self, other: "APIVersion") -> bool:
        """Check if versions are compatible (same major)."""
        return self.major == other.major
    
    def __lt__(self, other: "APIVersion") -> bool:
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)
    
    def __gt__(self, other: "APIVersion") -> bool:
        return (self.major, self.minor, self.patch) > (other.major, other.minor, other.patch)


@dataclass
class DeprecationInfo:
    """Deprecation information for a version."""
    
    version: APIVersion
    deprecated_at: datetime
    sunset_date: Optional[datetime] = None
    migration_guide: Optional[str] = None
    replacement_version: Optional[APIVersion] = None


@dataclass
class VersioningConfig:
    """Configuration for API versioning."""
    
    default_version: APIVersion = field(default_factory=lambda: APIVersion(major=1))
    supported_versions: List[APIVersion] = field(default_factory=list)
    header_name: str = "API-Version"
    accept_header_name: str = "Accept"
    scheme: VersionScheme = VersionScheme.SEMVER
    deprecation_notice_header: str = "X-API-Deprecation"
    sunset_policy_days: int = 180


class VersionRouter:
    """Routes requests to appropriate version handlers."""
    
    def __init__(self, config: VersioningConfig):
        self.config = config
        self._handlers: Dict[str, Callable] = {}
        self._deprecations: Dict[str, DeprecationInfo] = {}
    
    def register_handler(self, version: APIVersion, handler: Callable) -> None:
        """Register a handler for a specific version."""
        self._handlers[version.raw] = handler
        if version not in self.config.supported_versions:
            self.config.supported_versions.append(version)
            self.config.supported_versions.sort()
    
    def register_deprecation(
        self,
        version: APIVersion,
        sunset_date: Optional[datetime] = None,
        migration_guide: Optional[str] = None,
        replacement: Optional[APIVersion] = None
    ) -> None:
        """Mark a version as deprecated."""
        self._deprecations[version.raw] = DeprecationInfo(
            version=version,
            deprecated_at=datetime.now(),
            sunset_date=sunset_date or datetime.now() + timedelta(days=self.config.sunset_policy_days),
            migration_guide=migration_guide,
            replacement_version=replacement
        )
    
    def route(self, version: APIVersion) -> Optional[Callable]:
        """Route to the best matching version handler."""
        if version.raw in self._handlers:
            return self._handlers[version.raw]
        
        for supported in sorted(self._handlers.keys(), reverse=True):
            if APIVersion.parse(supported) <= version:
                return self._handlers[supported]
        
        return self._handlers.get(self.config.default_version.raw)
    
    def get_deprecation_headers(self, version: APIVersion) -> Dict[str, str]:
        """Get deprecation-related headers."""
        headers = {}
        deprecation = self._deprecations.get(version.raw)
        
        if deprecation:
            headers[self.config.deprecation_notice_header] = "true"
            headers["Deprecation"] = deprecation.deprecated_at.isoformat()
            
            if deprecation.sunset_date:
                headers["Sunset"] = deprecation.sunset_date.isoformat()
            
            if deprecation.migration_guide:
                headers["Migration-Guide"] = deprecation.migration_guide
            
            if deprecation.replacement_version:
                headers["X-Replacement-Version"] = deprecation.replacement_version.raw
        
        return headers


class APIVersioningAction:
    """
    Main API versioning action handler.
    
    Manages version negotiation, deprecation, and routing
    for multi-version API support.
    """
    
    def __init__(self, config: Optional[VersioningConfig] = None):
        self.config = config or VersioningConfig()
        self.router = VersionRouter(self.config)
        self._middleware: List[Callable] = []
        self._version_detectors: List[Callable] = []
    
    def add_middleware(self, func: Callable) -> None:
        """Add versioning middleware."""
        self._middleware.append(func)
    
    def add_version_detector(self, detector: Callable[[Dict], Optional[str]]) -> None:
        """Add a version detector function."""
        self._version_detectors.append(detector)
    
    def detect_version(self, request: Dict) -> APIVersion:
        """Detect API version from request."""
        for detector in self._version_detectors:
            version_str = detector(request)
            if version_str:
                return APIVersion.parse(version_str)
        
        headers = request.get("headers", {})
        
        version_header = headers.get(self.config.header_name)
        if version_header:
            return APIVersion.parse(version_header)
        
        accept = headers.get(self.config.accept_header_name, "")
        if "version=" in accept.lower():
            match = re.search(r'version=([\d.]+|v[\d.]+)', accept, re.I)
            if match:
                return APIVersion.parse(match.group(1))
        
        path = request.get("path", "")
        match = re.search(r'/v(\d+)(?:/|$)', path)
        if match:
            return APIVersion(major=int(match.group(1)))
        
        return self.config.default_version
    
    def select_best_version(
        self,
        requested: APIVersion,
        supported: List[APIVersion]
    ) -> Tuple[APIVersion, bool]:
        """
        Select the best matching version.
        
        Returns (version, is_exact_match).
        """
        if requested in supported:
            return requested, True
        
        exact_major = [v for v in supported if v.major == requested.major]
        if exact_major:
            best = max(exact_major)
            return best, False
        
        if supported:
            best = max(supported)
            return best, False
        
        return self.config.default_version, False
    
    async def process_request(self, request: Dict) -> Dict[str, Any]:
        """Process request with version detection and routing."""
        version = self.detect_version(request)
        handler = self.router.route(version)
        
        deprecation_headers = self.router.get_deprecation_headers(version)
        
        result = {
            "version": version.raw,
            "handler": handler,
            "deprecation_headers": deprecation_headers,
            "request": request
        }
        
        for mw in self._middleware:
            mw_result = mw(result)
            if asyncio.iscoroutine(mw_result):
                await mw_result
        
        return result
    
    def get_supported_versions(self) -> List[str]:
        """Get list of supported version strings."""
        return [v.raw for v in sorted(self.config.supported_versions)]
    
    def is_version_deprecated(self, version: APIVersion) -> bool:
        """Check if a version is deprecated."""
        return version.raw in self._deprecations
    
    def get_version_status(self, version: APIVersion) -> Dict[str, Any]:
        """Get detailed status for a version."""
        status = {
            "version": version.raw,
            "supported": version.raw in self._handlers,
            "deprecated": self.is_version_deprecated(version),
            "sunset_date": None,
            "replacement": None
        }
        
        deprecation = self._deprecations.get(version.raw)
        if deprecation:
            status["sunset_date"] = deprecation.sunset_date.isoformat() if deprecation.sunset_date else None
            status["replacement"] = deprecation.replacement_version.raw if deprecation.replacement_version else None
        
        return status


import asyncio
