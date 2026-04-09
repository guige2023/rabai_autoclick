"""API catalog and discovery for automation workflows.

Provides API registry, documentation generation, and endpoint
discovery capabilities for managing API assets.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import copy


class APIProtocol(Enum):
    """API protocol types."""
    REST = "rest"
    GRAPHQL = "graphql"
    WEBSOCKET = "websocket"
    GRPC = "grpc"
    WEBHOOK = "webhook"


class APIStatus(Enum):
    """API endpoint status."""
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    RETIRED = "retired"
    BETA = "beta"
    INTERNAL = "internal"


class AuthType(Enum):
    """Authentication types."""
    NONE = "none"
    API_KEY = "api_key"
    OAUTH2 = "oauth2"
    BASIC = "basic"
    BEARER = "bearer"
    JWT = "jwt"


@dataclass
class APIEndpoint:
    """A single API endpoint definition."""
    endpoint_id: str
    path: str
    method: str
    protocol: APIProtocol
    description: str
    status: APIStatus
    auth_type: AuthType
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    version: str = "1.0"
    tags: Set[str] = field(default_factory=set)
    deprecated_at: Optional[float] = None
    retired_at: Optional[float] = None
    owner: Optional[str] = None
    contact: Optional[str] = None
    rate_limit: Optional[int] = None
    timeout_seconds: Optional[int] = None
    retry_policy: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class APISchema:
    """Schema definition for API requests/responses."""
    schema_id: str
    name: str
    type: str
    properties: Dict[str, Any] = field(default_factory=dict)
    required: List[str] = field(default_factory=list)
    description: Optional[str] = None
    example: Optional[Dict[str, Any]] = None


@dataclass
class APIVersion:
    """Version information for an API."""
    version: str
    released_at: float
    changelog: str
    breaking: bool = False
    deprecation_date: Optional[float] = None


class APICatalog:
    """Central registry for API endpoints."""

    def __init__(self):
        self._endpoints: Dict[str, APIEndpoint] = {}
        self._schemas: Dict[str, APISchema] = {}
        self._versions: Dict[str, List[APIVersion]] = {}
        self._lock = threading.RLock()
        self._search_index: Dict[str, Set[str]] = {}

    def register_endpoint(
        self,
        path: str,
        method: str,
        protocol: APIProtocol,
        description: str,
        status: APIStatus = APIStatus.ACTIVE,
        auth_type: AuthType = AuthType.NONE,
        version: str = "1.0",
        tags: Optional[Set[str]] = None,
        owner: Optional[str] = None,
        contact: Optional[str] = None,
        rate_limit: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        retry_policy: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a new API endpoint."""
        endpoint_id = str(uuid.uuid4())[:12]

        endpoint = APIEndpoint(
            endpoint_id=endpoint_id,
            path=path,
            method=method.upper(),
            protocol=protocol,
            description=description,
            status=status,
            auth_type=auth_type,
            version=version,
            tags=tags or set(),
            owner=owner,
            contact=contact,
            rate_limit=rate_limit,
            timeout_seconds=timeout_seconds,
            retry_policy=retry_policy,
            metadata=metadata or {},
        )

        with self._lock:
            self._endpoints[endpoint_id] = endpoint
            self._update_search_index(endpoint)

        return endpoint_id

    def _update_search_index(self, endpoint: APIEndpoint) -> None:
        """Update search index with endpoint terms."""
        terms = set()

        terms.add(endpoint.path.lower())
        terms.add(endpoint.method.lower())
        terms.add(endpoint.protocol.value)
        terms.add(endpoint.description.lower())
        terms.add(endpoint.status.value)

        for tag in endpoint.tags:
            terms.add(tag.lower())

        if endpoint.owner:
            terms.add(endpoint.owner.lower())

        for term in terms:
            if term not in self._search_index:
                self._search_index[term] = set()
            self._search_index[term].add(endpoint.endpoint_id)

    def get_endpoint(self, endpoint_id: str) -> Optional[APIEndpoint]:
        """Get an endpoint by ID."""
        with self._lock:
            return copy.deepcopy(self._endpoints.get(endpoint_id))

    def get_by_path(self, path: str, method: str) -> Optional[APIEndpoint]:
        """Get an endpoint by path and method."""
        with self._lock:
            for endpoint in self._endpoints.values():
                if endpoint.path == path and endpoint.method == method.upper():
                    return copy.deepcopy(endpoint)
        return None

    def search(
        self,
        query: str,
        tags: Optional[Set[str]] = None,
        protocol: Optional[APIProtocol] = None,
        status: Optional[APIStatus] = None,
    ) -> List[APIEndpoint]:
        """Search for endpoints by query and filters."""
        query_lower = query.lower()
        query_terms = set(query_lower.split())

        with self._lock:
            candidates = set()

            if query:
                for term in query_terms:
                    if term in self._search_index:
                        if not candidates:
                            candidates = self._search_index[term].copy()
                        else:
                            candidates &= self._search_index[term]

                if not candidates:
                    for endpoint in self._endpoints.values():
                        if (query_lower in endpoint.path.lower() or
                            query_lower in endpoint.description.lower()):
                            candidates.add(endpoint.endpoint_id)

            else:
                candidates = set(self._endpoints.keys())

            results = []
            for endpoint_id in candidates:
                endpoint = self._endpoints.get(endpoint_id)
                if not endpoint:
                    continue

                if protocol and endpoint.protocol != protocol:
                    continue
                if status and endpoint.status != status:
                    continue
                if tags and not endpoint.tags.intersection(tags):
                    continue

                results.append(copy.deepcopy(endpoint))

            results.sort(key=lambda e: e.updated_at, reverse=True)
            return results

    def deprecate_endpoint(self, endpoint_id: str) -> bool:
        """Mark an endpoint as deprecated."""
        with self._lock:
            endpoint = self._endpoints.get(endpoint_id)
            if not endpoint:
                return False
            endpoint.status = APIStatus.DEPRECATED
            endpoint.deprecated_at = time.time()
            endpoint.updated_at = time.time()
            return True

    def retire_endpoint(self, endpoint_id: str) -> bool:
        """Mark an endpoint as retired."""
        with self._lock:
            endpoint = self._endpoints.get(endpoint_id)
            if not endpoint:
                return False
            endpoint.status = APIStatus.RETIRED
            endpoint.retired_at = time.time()
            endpoint.updated_at = time.time()
            return True

    def register_schema(
        self,
        name: str,
        schema_type: str,
        properties: Dict[str, Any],
        required: Optional[List[str]] = None,
        description: Optional[str] = None,
        example: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Register a schema definition."""
        schema_id = str(uuid.uuid4())[:12]

        schema = APISchema(
            schema_id=schema_id,
            name=name,
            type=schema_type,
            properties=properties,
            required=required or [],
            description=description,
            example=example,
        )

        with self._lock:
            self._schemas[schema_id] = schema

        return schema_id

    def get_schema(self, schema_id: str) -> Optional[APISchema]:
        """Get a schema by ID."""
        with self._lock:
            return copy.deepcopy(self._schemas.get(schema_id))

    def add_version(
        self,
        version: str,
        changelog: str,
        breaking: bool = False,
        deprecation_date: Optional[float] = None,
    ) -> None:
        """Add a new API version."""
        api_version = APIVersion(
            version=version,
            released_at=time.time(),
            changelog=changelog,
            breaking=breaking,
            deprecation_date=deprecation_date,
        )

        with self._lock:
            if version not in self._versions:
                self._versions[version] = []
            self._versions[version].append(api_version)

    def get_versions(self) -> List[Dict[str, Any]]:
        """Get all API versions."""
        with self._lock:
            result = []
            for version, versions in self._versions.items():
                if versions:
                    latest = versions[-1]
                    result.append({
                        "version": version,
                        "released_at": datetime.fromtimestamp(latest.released_at).isoformat(),
                        "changelog": latest.changelog,
                        "breaking": latest.breaking,
                        "deprecation_date": (
                            datetime.fromtimestamp(latest.deprecation_date).isoformat()
                            if latest.deprecation_date else None
                        ),
                    })
            return result

    def generate_openapi_spec(self) -> Dict[str, Any]:
        """Generate an OpenAPI specification from registered endpoints."""
        with self._lock:
            paths = {}
            for endpoint in self._endpoints.values():
                if endpoint.status == APIStatus.RETIRED:
                    continue

                if endpoint.path not in paths:
                    paths[endpoint.path] = {}

                path_item = paths[endpoint.path]
                path_item[endpoint.method.lower()] = {
                    "summary": endpoint.description,
                    "tags": list(endpoint.tags),
                    "responses": {
                        "200": {"description": "Successful response"},
                        "400": {"description": "Bad request"},
                        "401": {"description": "Unauthorized"},
                        "500": {"description": "Internal server error"},
                    },
                    "security": [{"bearerAuth": []}] if endpoint.auth_type != AuthType.NONE else [],
                }

            return {
                "openapi": "3.0.0",
                "info": {
                    "title": "API Catalog",
                    "version": "1.0.0",
                    "description": "Auto-generated API catalog",
                },
                "paths": paths,
            }


class AutomationCatalogAction:
    """Action providing API catalog capabilities."""

    def __init__(self, catalog: Optional[APICatalog] = None):
        self._catalog = catalog or APICatalog()

    def register(
        self,
        path: str,
        method: str,
        description: str,
        protocol: str = "rest",
        status: str = "active",
        auth_type: str = "none",
        version: str = "1.0",
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
        rate_limit: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Register a new API endpoint."""
        try:
            protocol_enum = APIProtocol(protocol.lower())
        except ValueError:
            protocol_enum = APIProtocol.REST

        try:
            status_enum = APIStatus(status.lower())
        except ValueError:
            status_enum = APIStatus.ACTIVE

        try:
            auth_enum = AuthType(auth_type.lower())
        except ValueError:
            auth_enum = AuthType.NONE

        endpoint_id = self._catalog.register_endpoint(
            path=path,
            method=method,
            protocol=protocol_enum,
            description=description,
            status=status_enum,
            auth_type=auth_enum,
            version=version,
            tags=set(tags) if tags else None,
            owner=owner,
            rate_limit=rate_limit,
            timeout_seconds=timeout_seconds,
        )

        return {
            "endpoint_id": endpoint_id,
            "path": path,
            "method": method,
            "status": status_enum.value,
        }

    def search(
        self,
        query: str,
        tags: Optional[List[str]] = None,
        protocol: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Search for API endpoints."""
        protocol_enum = APIProtocol(protocol.lower()) if protocol else None
        status_enum = APIStatus(status.lower()) if status else None

        results = self._catalog.search(
            query=query,
            tags=set(tags) if tags else None,
            protocol=protocol_enum,
            status=status_enum,
        )

        return [
            {
                "endpoint_id": e.endpoint_id,
                "path": e.path,
                "method": e.method,
                "protocol": e.protocol.value,
                "status": e.status.value,
                "description": e.description,
                "version": e.version,
                "tags": list(e.tags),
                "owner": e.owner,
            }
            for e in results
        ]

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a catalog operation.

        Required params:
            operation: str - 'register', 'search', 'get', 'deprecate', 'retire', 'generate_spec'
        """
        operation = params.get("operation")

        if operation == "register":
            return self.register(
                path=params.get("path"),
                method=params.get("method", "GET"),
                description=params.get("description", ""),
                protocol=params.get("protocol", "rest"),
                status=params.get("status", "active"),
                auth_type=params.get("auth_type", "none"),
                version=params.get("version", "1.0"),
                tags=params.get("tags"),
                owner=params.get("owner"),
                rate_limit=params.get("rate_limit"),
                timeout_seconds=params.get("timeout_seconds"),
            )

        elif operation == "search":
            return {
                "results": self.search(
                    query=params.get("query", ""),
                    tags=params.get("tags"),
                    protocol=params.get("protocol"),
                    status=params.get("status"),
                )
            }

        elif operation == "get":
            endpoint = self._catalog.get_endpoint(params.get("endpoint_id"))
            if not endpoint:
                return {"error": "Endpoint not found"}
            return {
                "endpoint_id": endpoint.endpoint_id,
                "path": endpoint.path,
                "method": endpoint.method,
                "protocol": endpoint.protocol.value,
                "status": endpoint.status.value,
                "description": endpoint.description,
                "version": endpoint.version,
                "auth_type": endpoint.auth_type.value,
                "tags": list(endpoint.tags),
                "owner": endpoint.owner,
                "rate_limit": endpoint.rate_limit,
                "timeout_seconds": endpoint.timeout_seconds,
            }

        elif operation == "deprecate":
            success = self._catalog.deprecate_endpoint(params.get("endpoint_id"))
            return {"success": success}

        elif operation == "retire":
            success = self._catalog.retire_endpoint(params.get("endpoint_id"))
            return {"success": success}

        elif operation == "generate_spec":
            spec = self._catalog.generate_openapi_spec()
            return {"spec": spec}

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def get_endpoint(self, endpoint_id: str) -> Optional[Dict[str, Any]]:
        """Get endpoint details."""
        endpoint = self._catalog.get_endpoint(endpoint_id)
        if not endpoint:
            return None
        return {
            "endpoint_id": endpoint.endpoint_id,
            "path": endpoint.path,
            "method": endpoint.method,
            "protocol": endpoint.protocol.value,
            "description": endpoint.description,
            "status": endpoint.status.value,
            "version": endpoint.version,
            "tags": list(endpoint.tags),
        }

    def get_all_endpoints(self) -> List[Dict[str, Any]]:
        """Get all registered endpoints."""
        with self._catalog._lock:
            return [
                {
                    "endpoint_id": e.endpoint_id,
                    "path": e.path,
                    "method": e.method,
                    "status": e.status.value,
                }
                for e in self._catalog._endpoints.values()
            ]
