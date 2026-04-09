"""Service catalog action module for RabAI AutoClick.

Provides service catalog operations:
- ServiceCatalog: Manage service catalog entries
- ServiceDiscovery: Discover services dynamically
- ServiceRegistry: Register and unregister services
- ServiceDependencyGraph: Map service dependencies
- ServiceHealthAggregator: Aggregate health status from multiple services
"""

from __future__ import annotations

import json
import sys
import os
import time
import socket
import hashlib
from typing import Any, Callable, Dict, List, Optional, Set, Union
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ServiceCatalogAction(BaseAction):
    """Manage service catalog entries."""
    action_type = "service_catalog"
    display_name = "服务目录"
    description = "管理服务目录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "register")
            catalog_path = params.get("catalog_path", "/tmp/service_catalog")
            service_name = params.get("service_name", "")
            service_endpoint = params.get("service_endpoint", "")
            metadata = params.get("metadata", {})
            tags = params.get("tags", [])

            os.makedirs(catalog_path, exist_ok=True)
            catalog_file = os.path.join(catalog_path, "catalog.json")

            catalog = {}
            if os.path.exists(catalog_file):
                with open(catalog_file) as f:
                    catalog = json.load(f)

            if operation == "register":
                if not service_name or not service_endpoint:
                    return ActionResult(success=False, message="service_name and service_endpoint required")

                entry = {
                    "name": service_name,
                    "endpoint": service_endpoint,
                    "metadata": metadata,
                    "tags": tags,
                    "registered_at": datetime.now().isoformat(),
                    "last_heartbeat": datetime.now().isoformat(),
                    "status": "healthy",
                    "version": metadata.get("version", "1.0.0"),
                }

                catalog[service_name] = entry
                with open(catalog_file, "w") as f:
                    json.dump(catalog, f, indent=2)

                return ActionResult(success=True, message=f"Registered: {service_name}", data={"service": service_name, "endpoint": service_endpoint})

            elif operation == "deregister":
                if not service_name:
                    return ActionResult(success=False, message="service_name required")

                if service_name in catalog:
                    del catalog[service_name]
                    with open(catalog_file, "w") as f:
                        json.dump(catalog, f, indent=2)

                return ActionResult(success=True, message=f"Deregistered: {service_name}")

            elif operation == "list":
                tag_filter = params.get("tag_filter", None)
                status_filter = params.get("status_filter", None)

                services = list(catalog.values())
                if tag_filter:
                    services = [s for s in services if tag_filter in s.get("tags", [])]
                if status_filter:
                    services = [s for s in services if s.get("status") == status_filter]

                return ActionResult(success=True, message=f"{len(services)} services", data={"services": services, "count": len(services)})

            elif operation == "get":
                if not service_name:
                    return ActionResult(success=False, message="service_name required")

                if service_name not in catalog:
                    return ActionResult(success=False, message=f"Service not found: {service_name}")

                return ActionResult(success=True, message=f"Service: {service_name}", data=catalog[service_name])

            elif operation == "update":
                if not service_name:
                    return ActionResult(success=False, message="service_name required")

                if service_name not in catalog:
                    return ActionResult(success=False, message=f"Service not found: {service_name}")

                catalog[service_name].update({
                    "metadata": metadata or catalog[service_name].get("metadata", {}),
                    "tags": tags or catalog[service_name].get("tags", []),
                    "endpoint": service_endpoint or catalog[service_name].get("endpoint", ""),
                    "last_updated": datetime.now().isoformat(),
                })

                with open(catalog_file, "w") as f:
                    json.dump(catalog, f, indent=2)

                return ActionResult(success=True, message=f"Updated: {service_name}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ServiceDiscoveryAction(BaseAction):
    """Discover services dynamically."""
    action_type = "service_discovery"
    display_name = "服务发现"
    description = "动态发现服务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "discover")
            catalog_path = params.get("catalog_path", "/tmp/service_catalog")
            service_name = params.get("service_name", "")
            tag = params.get("tag", None)
            healthy_only = params.get("healthy_only", True)
            catalog_file = os.path.join(catalog_path, "catalog.json")

            if not os.path.exists(catalog_file):
                return ActionResult(success=False, message="Catalog not initialized")

            with open(catalog_file) as f:
                catalog = json.load(f)

            if operation == "discover":
                if tag:
                    candidates = {k: v for k, v in catalog.items() if tag in v.get("tags", [])}
                elif service_name:
                    candidates = {k: v for k, v in catalog.items() if service_name in k}
                else:
                    candidates = catalog

                if healthy_only:
                    candidates = {k: v for k, v in candidates.items() if v.get("status") == "healthy"}

                if not candidates:
                    return ActionResult(success=False, message="No services found", data={"services": []})

                return ActionResult(
                    success=True,
                    message=f"Discovered {len(candidates)} services",
                    data={"services": list(candidates.values()), "count": len(candidates)}
                )

            elif operation == "resolve":
                if not service_name:
                    return ActionResult(success=False, message="service_name required")

                if service_name not in catalog:
                    return ActionResult(success=False, message=f"Service not found: {service_name}")

                entry = catalog[service_name]
                endpoint = entry.get("endpoint", "")

                if endpoint.startswith("http://") or endpoint.startswith("https://"):
                    resolved_url = endpoint
                else:
                    parts = endpoint.split(":")
                    host = parts[0]
                    port = int(parts[1]) if len(parts) > 1 else 80
                    try:
                        resolved_ip = socket.gethostbyname(host)
                        resolved_url = f"http://{resolved_ip}:{port}"
                    except:
                        resolved_url = f"http://{host}:{port}"

                return ActionResult(success=True, message=f"Resolved: {service_name}", data={"endpoint": endpoint, "resolved_url": resolved_url, "status": entry.get("status")})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ServiceDependencyGraphAction(BaseAction):
    """Map service dependencies."""
    action_type = "service_dependency_graph"
    display_name = "服务依赖图"
    description = "构建服务依赖关系图"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add_dependency")
            catalog_path = params.get("catalog_path", "/tmp/service_catalog")
            service_name = params.get("service_name", "")
            depends_on = params.get("depends_on", [])
            dep_file = os.path.join(catalog_path, "dependencies.json")

            os.makedirs(catalog_path, exist_ok=True)
            deps = {}
            if os.path.exists(dep_file):
                with open(dep_file) as f:
                    deps = json.load(f)

            if operation == "add_dependency":
                if not service_name:
                    return ActionResult(success=False, message="service_name required")

                if service_name not in deps:
                    deps[service_name] = {"depends_on": [], "depended_by": []}

                for dep in depends_on:
                    if dep not in deps[service_name]["depends_on"]:
                        deps[service_name]["depends_on"].append(dep)
                    if service_name not in deps:
                        deps[service_name] = {"depends_on": [], "depended_by": []}
                    if dep not in deps[service_name]["depended_by"]:
                        deps[dep]["depended_by"].append(service_name)

                with open(dep_file, "w") as f:
                    json.dump(deps, f, indent=2)

                return ActionResult(success=True, message=f"Added dependencies for {service_name}")

            elif operation == "get_dependencies":
                if not service_name:
                    return ActionResult(success=False, message="service_name required")

                if service_name not in deps:
                    return ActionResult(success=True, message="No dependencies", data={"depends_on": [], "depended_by": []})

                return ActionResult(
                    success=True,
                    message=f"Dependencies for {service_name}",
                    data={"depends_on": deps[service_name].get("depends_on", []), "depended_by": deps[service_name].get("depended_by", [])}
                )

            elif operation == "topological_sort":
                visited = set()
                sorted_services = []

                def visit(svc):
                    if svc in visited:
                        return
                    visited.add(svc)
                    if svc in deps:
                        for dep in deps[svc].get("depends_on", []):
                            visit(dep)
                    sorted_services.append(svc)

                for svc in deps:
                    visit(svc)

                return ActionResult(success=True, message=f"Sorted {len(sorted_services)} services", data={"order": sorted_services})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class ServiceHealthAggregatorAction(BaseAction):
    """Aggregate health status from multiple services."""
    action_type = "service_health_aggregator"
    display_name = "服务健康聚合"
    description = "聚合多个服务的健康状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "aggregate")
            catalog_path = params.get("catalog_path", "/tmp/service_catalog")
            catalog_file = os.path.join(catalog_path, "catalog.json")
            service_name = params.get("service_name", "")
            health_status = params.get("health_status", "healthy")

            if not os.path.exists(catalog_file):
                return ActionResult(success=False, message="Catalog not initialized")

            with open(catalog_file) as f:
                catalog = json.load(f)

            if operation == "update_health":
                if not service_name or not health_status:
                    return ActionResult(success=False, message="service_name and health_status required")

                valid_statuses = ["healthy", "degraded", "unhealthy", "unknown"]
                if health_status not in valid_statuses:
                    return ActionResult(success=False, message=f"Invalid status. Use: {valid_statuses}")

                if service_name in catalog:
                    catalog[service_name]["status"] = health_status
                    catalog[service_name]["last_heartbeat"] = datetime.now().isoformat()
                    with open(catalog_file, "w") as f:
                        json.dump(catalog, f, indent=2)

                return ActionResult(success=True, message=f"Updated health: {service_name} -> {health_status}")

            elif operation == "aggregate":
                health_counts = defaultdict(int)
                for svc in catalog.values():
                    status = svc.get("status", "unknown")
                    health_counts[status] += 1

                total = len(catalog)
                overall = "healthy"
                if health_counts.get("unhealthy", 0) > 0:
                    overall = "unhealthy"
                elif health_counts.get("degraded", 0) > 0:
                    overall = "degraded"

                return ActionResult(
                    success=True,
                    message=f"Overall health: {overall}",
                    data={
                        "overall_health": overall,
                        "total_services": total,
                        "healthy_count": health_counts.get("healthy", 0),
                        "degraded_count": health_counts.get("degraded", 0),
                        "unhealthy_count": health_counts.get("unhealthy", 0),
                        "unknown_count": health_counts.get("unknown", 0),
                    }
                )

            elif operation == "get_unhealthy":
                unhealthy = [s for s in catalog.values() if s.get("status") in ("degraded", "unhealthy")]
                return ActionResult(success=True, message=f"{len(unhealthy)} unhealthy services", data={"unhealthy_services": unhealthy})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
