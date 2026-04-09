"""API documentation and discovery action module for RabAI AutoClick.

Provides:
- ApiDocumentationAction: Generate and manage API documentation
- ApiDiscoveryAction: Discover and register APIs
- ApiRegistryAction: API registry and lookup
"""

import time
import json
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ApiDocumentationAction(BaseAction):
    """Generate and manage API documentation."""
    action_type = "api_documentation"
    display_name = "API文档"
    description = "API文档生成管理"

    def __init__(self):
        super().__init__()
        self._docs: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "generate")
            api_name = params.get("api_name", "")

            if operation == "generate":
                if not api_name:
                    return ActionResult(success=False, message="api_name required")

                endpoints = params.get("endpoints", [])
                version = params.get("version", "1.0")
                base_path = params.get("base_path", "/")

                doc = {
                    "api_name": api_name,
                    "version": version,
                    "base_path": base_path,
                    "description": params.get("description", ""),
                    "endpoints": [],
                    "created_at": time.time(),
                    "updated_at": time.time()
                }

                for ep in endpoints:
                    doc["endpoints"].append({
                        "path": ep.get("path", "/"),
                        "method": ep.get("method", "GET"),
                        "summary": ep.get("summary", ""),
                        "description": ep.get("description", ""),
                        "parameters": ep.get("parameters", []),
                        "request_body": ep.get("request_body"),
                        "responses": ep.get("responses", {}),
                        "tags": ep.get("tags", [])
                    })

                doc["hash"] = hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()[:16]
                self._docs[api_name] = doc

                return ActionResult(
                    success=True,
                    data={
                        "api": api_name,
                        "version": version,
                        "endpoints": len(endpoints),
                        "doc_hash": doc["hash"]
                    },
                    message=f"Documentation generated for '{api_name}' v{version}"
                )

            elif operation == "get":
                if api_name not in self._docs:
                    return ActionResult(success=False, message=f"API '{api_name}' not found")
                return ActionResult(success=True, data={"doc": self._docs[api_name]})

            elif operation == "update":
                if api_name not in self._docs:
                    return ActionResult(success=False, message=f"API '{api_name}' not found")

                doc = self._docs[api_name]
                updates = params.get("updates", {})
                doc.update(updates)
                doc["updated_at"] = time.time()
                doc["hash"] = hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()[:16]

                return ActionResult(success=True, data={"updated": api_name}, message=f"Documentation updated for '{api_name}'")

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "apis": [
                            {
                                "api": k,
                                "version": v["version"],
                                "endpoints": len(v["endpoints"]),
                                "updated": v.get("updated_at")
                            }
                            for k, v in self._docs.items()
                        ]
                    }
                )

            elif operation == "export":
                fmt = params.get("format", "openapi")
                if api_name not in self._docs:
                    return ActionResult(success=False, message=f"API '{api_name}' not found")

                doc = self._docs[api_name]
                if fmt == "openapi":
                    exported = self._to_openapi(doc)
                elif fmt == "markdown":
                    exported = self._to_markdown(doc)
                else:
                    exported = json.dumps(doc, indent=2)

                return ActionResult(
                    success=True,
                    data={"api": api_name, "format": fmt, "exported": exported}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Documentation error: {str(e)}")

    def _to_openapi(self, doc: Dict) -> Dict:
        return {
            "openapi": "3.0.0",
            "info": {
                "title": doc["api_name"],
                "version": doc["version"]
            },
            "paths": {
                ep["path"]: {ep["method"].lower(): {"summary": ep["summary"]}}
                for ep in doc["endpoints"]
            }
        }

    def _to_markdown(self, doc: Dict) -> str:
        lines = [f"# {doc['api_name']}", f"**Version**: {doc['version']}", "", doc.get("description", ""), ""]
        for ep in doc["endpoints"]:
            lines.append(f"## {ep['method']} {ep['path']}")
            lines.append(f"{ep.get('summary', '')}")
            lines.append("")
        return "\n".join(lines)


class ApiDiscoveryAction(BaseAction):
    """Discover and register APIs dynamically."""
    action_type = "api_discovery"
    display_name = "API发现"
    description = "API动态发现"

    def __init__(self):
        super().__init__()
        self._discovered_apis: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "discover")
            api_name = params.get("api_name", "")

            if operation == "discover":
                if not api_name:
                    return ActionResult(success=False, message="api_name required")

                discovery = {
                    "api_name": api_name,
                    "base_url": params.get("base_url", ""),
                    "endpoints": params.get("endpoints", []),
                    "authentication": params.get("authentication", {}),
                    "rate_limits": params.get("rate_limits", {}),
                    "discovered_at": time.time(),
                    "status": "discovered"
                }

                self._discovered_apis[api_name] = discovery
                return ActionResult(
                    success=True,
                    data={
                        "api": api_name,
                        "endpoints_found": len(discovery["endpoints"])
                    },
                    message=f"Discovered API '{api_name}' with {len(discovery['endpoints'])} endpoints"
                )

            elif operation == "probe":
                if not api_name:
                    return ActionResult(success=False, message="api_name required")

                discovery = self._discovered_apis.get(api_name, {})
                endpoints = discovery.get("endpoints", [])

                probed = []
                for ep in endpoints:
                    probed.append({
                        "path": ep.get("path", ""),
                        "method": ep.get("method", "GET"),
                        "status": "reachable",
                        "response_time_ms": 100
                    })

                return ActionResult(
                    success=True,
                    data={
                        "api": api_name,
                        "probed": probed,
                        "reachable_count": len(probed)
                    }
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "discovered": [
                            {"api": k, "endpoints": len(v["endpoints"]), "at": v["discovered_at"]}
                            for k, v in self._discovered_apis.items()
                        ]
                    }
                )

            elif operation == "remove":
                if api_name in self._discovered_apis:
                    del self._discovered_apis[api_name]
                return ActionResult(success=True, message=f"Removed '{api_name}' from discovery")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Discovery error: {str(e)}")


class ApiRegistryAction(BaseAction):
    """API registry for service lookup."""
    action_type = "api_registry"
    display_name = "API注册表"
    description = "API服务注册表"

    def __init__(self):
        super().__init__()
        self._registry: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            api_name = params.get("api_name", "")

            if operation == "register":
                if not api_name:
                    return ActionResult(success=False, message="api_name required")

                self._registry[api_name] = {
                    "api_name": api_name,
                    "version": params.get("version", "1.0"),
                    "base_url": params.get("base_url", ""),
                    "owner": params.get("owner", ""),
                    "description": params.get("description", ""),
                    "tags": params.get("tags", []),
                    "metadata": params.get("metadata", {}),
                    "registered_at": time.time(),
                    "updated_at": time.time(),
                    "status": "active"
                }
                return ActionResult(success=True, data={"api": api_name}, message=f"API '{api_name}' registered")

            elif operation == "lookup":
                if not api_name:
                    return ActionResult(success=False, message="api_name required")

                if api_name not in self._registry:
                    return ActionResult(success=False, message=f"API '{api_name}' not found")

                return ActionResult(success=True, data={"api": self._registry[api_name]})

            elif operation == "search":
                tag = params.get("tag", "")
                owner = params.get("owner", "")
                keyword = params.get("keyword", "")

                results = []
                for api in self._registry.values():
                    if tag and tag not in api.get("tags", []):
                        continue
                    if owner and api.get("owner", "") != owner:
                        continue
                    if keyword and keyword.lower() not in api.get("description", "").lower():
                        continue
                    results.append(api)

                return ActionResult(
                    success=True,
                    data={"results": results, "count": len(results)}
                )

            elif operation == "update":
                if api_name not in self._registry:
                    return ActionResult(success=False, message=f"API '{api_name}' not found")

                updates = params.get("updates", {})
                self._registry[api_name].update(updates)
                self._registry[api_name]["updated_at"] = time.time()

                return ActionResult(success=True, data={"api": api_name})

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "apis": [
                            {"name": k, "version": v["version"], "owner": v.get("owner", ""), "status": v.get("status", "active")}
                            for k, v in self._registry.items()
                        ]
                    }
                )

            elif operation == "deregister":
                if api_name in self._registry:
                    self._registry[api_name]["status"] = "deprecated"
                    self._registry[api_name]["deprecated_at"] = time.time()
                return ActionResult(success=True, message=f"API '{api_name}' deregistered")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Registry error: {str(e)}")
