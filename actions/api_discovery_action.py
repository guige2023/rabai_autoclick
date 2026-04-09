"""API discovery action module for RabAI AutoClick.

Provides API discovery operations:
- ApiDiscoveryAction: Discover APIs from configuration
- ApiServiceRegistryAction: Register and lookup services
- ApiEndpointDiscoveryAction: Discover endpoints dynamically
- ApiVersionDiscoveryAction: Discover available API versions
"""

import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiDiscoveryAction(BaseAction):
    """Discover APIs from OpenAPI/Swagger specs or URLs."""
    action_type = "api_discovery"
    display_name = "API发现"
    description = "从配置或规范发现API"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source")
            source_type = params.get("source_type", "url")
            filter_methods = params.get("filter_methods", [])
            filter_tags = params.get("filter_tags", [])

            if not source:
                return ActionResult(success=False, message="source is required")

            discovered_apis = []

            if source_type == "url":
                discovered_apis = self._discover_from_url(source, filter_methods, filter_tags)
            elif source_type == "openapi":
                discovered_apis = self._parse_openapi_spec(source, filter_methods, filter_tags)
            elif source_type == "endpoints":
                discovered_apis = self._discover_from_list(source, filter_methods, filter_tags)
            else:
                return ActionResult(success=False, message=f"Unknown source_type: {source_type}")

            return ActionResult(
                success=True,
                message=f"Discovered {len(discovered_apis)} API endpoints",
                data={"apis": discovered_apis, "count": len(discovered_apis)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"API discovery error: {e}")

    def _discover_from_url(self, url: str, filter_methods: List[str], filter_tags: List[str]) -> List[Dict[str, Any]]:
        """Fetch and parse API spec from URL."""
        try:
            import urllib.request
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as response:
                content = response.read().decode()

            try:
                import json
                spec = json.loads(content)
                if "paths" in spec:
                    return self._parse_openapi_spec(spec, filter_methods, filter_tags)
            except json.JSONDecodeError:
                pass

            return self._discover_from_text(content, filter_methods, filter_tags)
        except Exception:
            return []

    def _parse_openapi_spec(self, spec: Any, filter_methods: List[str], filter_tags: List[str]) -> List[Dict[str, Any]]:
        """Parse OpenAPI spec."""
        if isinstance(spec, str):
            try:
                import json
                spec = json.loads(spec)
            except Exception:
                return []

        paths = spec.get("paths", {})
        results = []

        for path, methods in paths.items():
            for method, details in methods.items():
                if method.upper() not in ("GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"):
                    continue

                if filter_methods and method.upper() not in [m.upper() for m in filter_methods]:
                    continue

                tags = details.get("tags", [])
                if filter_tags and not any(t in tags for t in filter_tags):
                    continue

                results.append({
                    "path": path,
                    "method": method.upper(),
                    "summary": details.get("summary", ""),
                    "description": details.get("description", ""),
                    "tags": tags,
                    "parameters": details.get("parameters", []),
                    "operation_id": details.get("operationId"),
                })

        return results

    def _discover_from_list(self, endpoints: List[str], filter_methods: List[str], filter_tags: List[str]) -> List[Dict[str, Any]]:
        """Discover from a list of endpoint strings."""
        results = []
        for endpoint in endpoints:
            if isinstance(endpoint, str):
                parts = endpoint.split()
                if len(parts) >= 2:
                    method, path = parts[0].upper(), parts[1]
                else:
                    method, path = "GET", parts[0]
                results.append({"method": method, "path": path, "summary": "", "tags": []})
            elif isinstance(endpoint, dict):
                results.append(endpoint)
        return results

    def _discover_from_text(self, text: str, filter_methods: List[str], filter_tags: List[str]) -> List[Dict[str, Any]]:
        """Discover endpoints from text."""
        results = []
        pattern = r"(GET|POST|PUT|DELETE|PATCH|OPTIONS|HEAD)\s+([^\s]+)"
        matches = re.findall(pattern, text, re.IGNORECASE)
        for method, path in matches:
            if filter_methods and method.upper() not in [m.upper() for m in filter_methods]:
                continue
            results.append({"method": method.upper(), "path": path, "summary": "", "tags": []})
        return results


class ApiServiceRegistryAction(BaseAction):
    """Register and lookup services."""
    action_type = "api_service_registry"
    display_name = "API服务注册表"
    description = "注册和查找服务"

    def __init__(self):
        super().__init__()
        self._registry: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            service_name = params.get("service_name")
            endpoint = params.get("endpoint")
            metadata = params.get("metadata", {})
            health_check_url = params.get("health_check_url")

            if operation == "register":
                if not service_name or not endpoint:
                    return ActionResult(success=False, message="service_name and endpoint are required")

                self._registry[service_name] = {
                    "endpoint": endpoint,
                    "metadata": metadata,
                    "health_check_url": health_check_url,
                    "registered_at": datetime.now().isoformat(),
                    "status": "active",
                }
                return ActionResult(success=True, message=f"Service {service_name} registered", data={"service_name": service_name})

            elif operation == "lookup":
                if not service_name:
                    return ActionResult(success=False, message="service_name is required")
                if service_name not in self._registry:
                    return ActionResult(success=False, message=f"Service {service_name} not found")
                return ActionResult(success=True, message=f"Found {service_name}", data=self._registry[service_name])

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._registry)} services registered", data={"services": self._registry})

            elif operation == "deregister":
                if service_name and service_name in self._registry:
                    del self._registry[service_name]
                    return ActionResult(success=True, message=f"Service {service_name} deregistered")
                return ActionResult(success=False, message=f"Service {service_name} not found")

            elif operation == "update":
                if not service_name or service_name not in self._registry:
                    return ActionResult(success=False, message="Service not found")
                if endpoint:
                    self._registry[service_name]["endpoint"] = endpoint
                self._registry[service_name]["metadata"].update(metadata)
                return ActionResult(success=True, message=f"Service {service_name} updated")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Service registry error: {e}")


class ApiEndpointDiscoveryAction(BaseAction):
    """Discover API endpoints dynamically."""
    action_type = "api_endpoint_discovery"
    display_name = "API端点发现"
    description = "动态发现API端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            paths_to_check = params.get("paths_to_check", ["/", "/api", "/v1", "/v2", "/health", "/ping", "/api-docs", "/swagger"])
            methods = params.get("methods", ["GET"])
            timeout = params.get("timeout", 5)

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            discovered = []

            for path in paths_to_check:
                url = base_url.rstrip("/") + "/" + path.lstrip("/")
                for method in methods:
                    try:
                        import urllib.request
                        req = urllib.request.Request(url, method=method)
                        try:
                            with urllib.request.urlopen(req, timeout=timeout) as response:
                                discovered.append({
                                    "url": url,
                                    "method": method,
                                    "status": response.status,
                                    "accessible": True,
                                })
                        except urllib.error.HTTPError as e:
                            discovered.append({
                                "url": url,
                                "method": method,
                                "status": e.code,
                                "accessible": True,
                                "error": str(e),
                            })
                    except Exception as e:
                        discovered.append({"url": url, "method": method, "accessible": False, "error": str(e)})

            accessible = [d for d in discovered if d.get("accessible", False)]
            return ActionResult(
                success=True,
                message=f"Discovered {len(accessible)} accessible endpoints",
                data={"discovered": discovered, "accessible_count": len(accessible), "total_checked": len(discovered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Endpoint discovery error: {e}")


class ApiVersionDiscoveryAction(BaseAction):
    """Discover available API versions."""
    action_type = "api_version_discovery"
    display_name = "API版本发现"
    description = "发现可用API版本"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            base_url = params.get("base_url", "")
            version_paths = params.get("version_paths", ["/v1", "/v2", "/v3", "/api/v1", "/api/v2", "/api/v3"])
            timeout = params.get("timeout", 5)

            if not base_url:
                return ActionResult(success=False, message="base_url is required")

            versions = {}

            for vp in version_paths:
                url = base_url.rstrip("/") + "/" + vp.lstrip("/")
                try:
                    import urllib.request
                    req = urllib.request.Request(url)
                    try:
                        with urllib.request.urlopen(req, timeout=timeout) as response:
                            content = response.read().decode()
                            versions[vp] = {
                                "accessible": True,
                                "status": response.status,
                                "version": self._extract_version_from_content(content, vp),
                            }
                    except urllib.error.HTTPError as e:
                        versions[vp] = {"accessible": True, "status": e.code, "version": None, "error": str(e)}
                except Exception as e:
                    versions[vp] = {"accessible": False, "version": None, "error": str(e)}

            supported = {k: v for k, v in versions.items() if v.get("accessible", False)}

            return ActionResult(
                success=True,
                message=f"Found {len(supported)} accessible versions",
                data={"versions": versions, "supported_versions": list(supported.keys()), "count": len(supported)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Version discovery error: {e}")

    def _extract_version_from_content(self, content: str, path_hint: str) -> Optional[str]:
        """Extract version from content."""
        version_match = re.search(r'"version"\s*:\s*"([^"]+)"', content)
        if version_match:
            return version_match.group(1)
        version_in_path = re.search(r'v(\d+)', path_hint)
        if version_in_path:
            return f"v{version_in_path.group(1)}"
        return None
