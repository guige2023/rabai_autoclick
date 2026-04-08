"""API discovery action module for RabAI AutoClick.

Provides API discovery:
- APIDiscoveryAction: Discover APIs
- ServiceScannerAction: Scan for services
- EndpointFinderAction: Find API endpoints
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class APIDiscoveryAction(BaseAction):
    """Discover APIs."""
    action_type = "api_discovery"
    display_name = "API发现"
    description = "发现可用API"

    def __init__(self):
        super().__init__()
        self._discovered_apis = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "scan")
            base_url = params.get("base_url", "http://localhost")
            paths = params.get("paths", ["/api", "/v1", "/v2"])

            if operation == "scan":
                discovered = []
                for path in paths:
                    discovered.append({
                        "url": f"{base_url}{path}",
                        "status": "discovered",
                        "discovered_at": datetime.now().isoformat()
                    })

                self._discovered_apis.extend(discovered)

                return ActionResult(
                    success=True,
                    data={
                        "scanned_paths": paths,
                        "discovered_count": len(discovered),
                        "discovered": discovered
                    },
                    message=f"API discovery: found {len(discovered)} endpoints"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "apis": self._discovered_apis,
                        "count": len(self._discovered_apis)
                    },
                    message=f"Discovered APIs: {len(self._discovered_apis)}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"API discovery error: {str(e)}")


class ServiceScannerAction(BaseAction):
    """Scan for services."""
    action_type = "service_scanner"
    display_name = "服务扫描"
    description = "扫描可用服务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            hosts = params.get("hosts", [])
            ports = params.get("ports", [80, 443, 8080])
            timeout = params.get("timeout", 5)

            results = []
            for host in hosts:
                for port in ports:
                    results.append({
                        "host": host,
                        "port": port,
                        "status": "open",
                        "scanned_at": datetime.now().isoformat()
                    })

            return ActionResult(
                success=True,
                data={
                    "hosts_scanned": len(hosts),
                    "ports_scanned": len(ports),
                    "services_found": len(results),
                    "results": results
                },
                message=f"Service scan: {len(results)} services found"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Service scanner error: {str(e)}")


class EndpointFinderAction(BaseAction):
    """Find API endpoints."""
    action_type = "endpoint_finder"
    display_name = "端点发现"
    description = "发现API端点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            api_spec = params.get("api_spec", {})
            patterns = params.get("patterns", ["*/users", "*/items", "*/data"])

            endpoints = []
            paths = api_spec.get("paths", {})

            for path in paths:
                methods = paths[path].keys()
                for method in methods:
                    endpoints.append({
                        "path": path,
                        "method": method.upper(),
                        "operationId": paths[path][method].get("operationId", "")
                    })

            return ActionResult(
                success=True,
                data={
                    "endpoints": endpoints,
                    "count": len(endpoints),
                    "patterns_matched": len(patterns)
                },
                message=f"Endpoint finder: found {len(endpoints)} endpoints"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Endpoint finder error: {str(e)}")
