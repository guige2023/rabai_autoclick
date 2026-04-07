"""
Istio Service Mesh Utilities.

Helpers for managing Istio VirtualServices, DestinationRules,
gateways, and traffic policies via the Kubernetes API or
Istio's control plane API.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import subprocess
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

ISTIO_NAMESPACE = os.getenv("ISTIO_NAMESPACE", "istio-system")
KUBECTL_PATH = os.getenv("KUBECTL_PATH", "kubectl")


def _run(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [KUBECTL_PATH] + args,
        capture_output=True,
        text=True,
    )


def _istio_args() -> list[str]:
    return ["-n", ISTIO_NAMESPACE]


# --------------------------------------------------------------------------- #
# Gateways
# --------------------------------------------------------------------------- #

def list_gateways() -> list[dict[str, Any]]:
    """Return all Istio Gateway resources."""
    result = _run(
        ["get", "gateway", "-o", "json"] + _istio_args()
    )
    if result.stdout.strip():
        data = json.loads(result.stdout)
        return data.get("items", [])
    return []


def apply_gateway(name: str, spec: dict[str, Any]) -> dict[str, Any]:
    """
    Apply a Gateway resource from a spec dict.

    Args:
        name: Gateway resource name.
        spec: Gateway spec (selector, servers, etc.).

    Returns:
        Applied resource.
    """
    manifest = {
        "apiVersion": "networking.istio.io/v1beta1",
        "kind": "Gateway",
        "metadata": {"name": name},
        "spec": spec,
    }
    import tempfile, pathlib
    with tempfile.NamedTemporaryFile(
        suffix=".yaml", mode="w", delete=False
    ) as f:
        import yaml
        yaml.safe_dump(manifest, f)
        path = f.name
    result = _run(["apply", "-f", path] + _istio_args())
    return {"name": name, "stdout": result.stdout, "stderr": result.stderr}


# --------------------------------------------------------------------------- #
# VirtualServices
# --------------------------------------------------------------------------- #

def list_virtual_services() -> list[dict[str, Any]]:
    """Return all VirtualService resources."""
    result = _run(
        ["get", "virtualservice", "-o", "json"] + _istio_args()
    )
    if result.stdout.strip():
        data = json.loads(result.stdout)
        return data.get("items", [])
    return []


def apply_virtual_service(
    name: str,
    spec: dict[str, Any],
) -> dict[str, Any]:
    """
    Apply a VirtualService resource.

    Args:
        name: VirtualService name.
        spec: VirtualService spec (hosts, gateways, http/tcp/tls routes).

    Returns:
        Applied resource status.
    """
    manifest = {
        "apiVersion": "networking.istio.io/v1beta1",
        "kind": "VirtualService",
        "metadata": {"name": name},
        "spec": spec,
    }
    import tempfile
    with tempfile.NamedTemporaryFile(
        suffix=".yaml", mode="w", delete=False
    ) as f:
        import yaml
        yaml.safe_dump(manifest, f)
        path = f.name
    result = _run(["apply", "-f", path] + _istio_args())
    return {"name": name, "stdout": result.stdout, "stderr": result.stderr}


# --------------------------------------------------------------------------- #
# DestinationRules
# --------------------------------------------------------------------------- #

def list_destination_rules() -> list[dict[str, Any]]:
    """Return all DestinationRule resources."""
    result = _run(
        ["get", "destinationrule", "-o", "json"] + _istio_args()
    )
    if result.stdout.strip():
        data = json.loads(result.stdout)
        return data.get("items", [])
    return []


def apply_destination_rule(
    name: str,
    host: str,
    traffic_policy: Optional[dict[str, Any]] = None,
    subsets: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Apply a DestinationRule resource.

    Args:
        name: DestinationRule name.
        host: Hostname the rule applies to.
        traffic_policy: Traffic policy (load balancer, connection pool, etc.).
        subsets: Named version subsets (e.g. version: v1, v2).
    """
    spec: dict[str, Any] = {"host": host}
    if traffic_policy:
        spec["trafficPolicy"] = traffic_policy
    if subsets:
        spec["subsets"] = subsets
    manifest = {
        "apiVersion": "networking.istio.io/v1beta1",
        "kind": "DestinationRule",
        "metadata": {"name": name},
        "spec": spec,
    }
    import tempfile
    with tempfile.NamedTemporaryFile(
        suffix=".yaml", mode="w", delete=False
    ) as f:
        import yaml
        yaml.safe_dump(manifest, f)
        path = f.name
    result = _run(["apply", "-f", path] + _istio_args())
    return {"name": name, "stdout": result.stdout, "stderr": result.stderr}


# --------------------------------------------------------------------------- #
# Traffic Management
# --------------------------------------------------------------------------- #

def set_weighted_routing(
    virtual_service: str,
    host: str,
    subsets: list[dict[str, Any]],
    gateways: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Set weighted routing between subsets (e.g. traffic split).

    Args:
        virtual_service: Name of the VirtualService.
        host: Destination host.
        subsets: List of dicts with 'name' and 'weight' keys.
        gateways: List of gateway references.
    """
    http_route: list[dict[str, Any]] = []
    for subset in subsets:
        http_route.append({
            "destination": {
                "host": host,
                "subset": subset["name"],
            },
            "weight": subset.get("weight", 100 // len(subsets)),
        })
    spec: dict[str, Any] = {
        "hosts": [host],
        "http": [{"route": http_route}],
    }
    if gateways:
        spec["gateways"] = gateways
    return apply_virtual_service(virtual_service, spec)


def add_timeout(
    virtual_service: str,
    route: list[dict[str, Any]],
    timeout: str = "5s",
) -> list[dict[str, Any]]:
    """Add a timeout to each route in a VirtualService spec."""
    for r in route:
        r["timeout"] = timeout
    return route


def add_retries(
    virtual_service: str,
    route: list[dict[str, Any]],
    attempts: int = 3,
    per_try_timeout: str = "2s",
) -> list[dict[str, Any]]:
    """Add retry policy to each route."""
    for r in route:
        r["retries"] = {
            "attempts": attempts,
            "perTryTimeout": per_try_timeout,
        }
    return route


# --------------------------------------------------------------------------- #
# Authorization
# --------------------------------------------------------------------------- #

def list_authorization_policies() -> list[dict[str, Any]]:
    """Return all AuthorizationPolicy resources."""
    result = _run(
        ["get", "authorizationpolicy", "-o", "json"] + _istio_args()
    )
    if result.stdout.strip():
        data = json.loads(result.stdout)
        return data.get("items", [])
    return []


def apply_authorization_policy(
    name: str,
    selector: dict[str, str],
    rules: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Apply an AuthorizationPolicy.

    Args:
        name: Policy name.
        selector: Workload selector (e.g. {"app": "myapp"}).
        rules: Istio authorization rules.
    """
    manifest = {
        "apiVersion": "security.istio.io/v1beta1",
        "kind": "AuthorizationPolicy",
        "metadata": {"name": name},
        "spec": {
            "selector": selector,
            "rules": rules,
        },
    }
    import tempfile
    with tempfile.NamedTemporaryFile(
        suffix=".yaml", mode="w", delete=False
    ) as f:
        import yaml
        yaml.safe_dump(manifest, f)
        path = f.name
    result = _run(["apply", "-f", path] + _istio_args())
    return {"name": name, "stdout": result.stdout, "stderr": result.stderr}


# --------------------------------------------------------------------------- #
# Telemetry
# --------------------------------------------------------------------------- #

def apply_telemetry(
    name: str,
    selector: dict[str, str],
    tracing_sampling: float = 100.0,
) -> dict[str, Any]:
    """
    Apply a Telemetry resource to configure tracing.

    Args:
        name: Telemetry resource name.
        selector: Workload selector.
        tracing_sampling: Trace sampling percentage (0-100).
    """
    manifest = {
        "apiVersion": "telemetry.istio.io/v1alpha1",
        "kind": "Telemetry",
        "metadata": {"name": name},
        "spec": {
            "selector": selector,
            "tracing": [
                {
                    "randomSamplingPercentage": tracing_sampling,
                }
            ],
        },
    }
    import tempfile
    with tempfile.NamedTemporaryFile(
        suffix=".yaml", mode="w", delete=False
    ) as f:
        import yaml
        yaml.safe_dump(manifest, f)
        path = f.name
    result = _run(["apply", "-f", path] + _istio_args())
    return {"name": name, "stdout": result.stdout, "stderr": result.stderr}
