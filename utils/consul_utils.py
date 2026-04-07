"""
HashiCorp Consul Service Discovery Utilities.

Helpers for registering/deregistering services, health checks,
KV store operations, and session management in Consul.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
import urllib.parse
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

CONSUL_HOST = os.getenv("CONSUL_HOST", "http://localhost:8500")
CONSUL_TOKEN = os.getenv("CONSUL_TOKEN", "")
CONSUL_API_BASE = CONSUL_HOST.rstrip("/") + "/v1"


def _headers() -> dict[str, str]:
    h: dict[str, str] = {"Content-Type": "application/json"}
    if CONSUL_TOKEN:
        h["X-Consul-Token"] = CONSUL_TOKEN
    return h


def _api(
    method: str,
    path: str,
    params: Optional[dict[str, str]] = None,
    body: Optional[dict[str, Any]] = None,
) -> tuple[int, Any]:
    url = f"{CONSUL_API_BASE}{path}"
    if params:
        qs = urllib.parse.urlencode(params)
        url = f"{url}?{qs}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            parsed = json.loads(raw) if raw else None
            return (resp.status, parsed)
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode()
        raise ConsulAPIError(exc.code, body_text) from exc


class ConsulAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Consul API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Agent / Service Registration
# --------------------------------------------------------------------------- #

def register_service(
    name: str,
    service_id: str,
    address: str,
    port: int,
    tags: Optional[list[str]] = None,
    check: Optional[dict[str, Any]] = None,
    meta: Optional[dict[str, str]] = None,
) -> bool:
    """
    Register a service with the local Consul agent.

    Args:
        name: Service name.
        service_id: Unique service ID.
        address: Service IP address.
        port: Service port.
        tags: Optional service tags.
        check: Optional health check definition.
        meta: Optional metadata dict.

    Returns:
        True if registration succeeded.
    """
    payload: dict[str, Any] = {
        "Name": name,
        "ID": service_id,
        "Address": address,
        "Port": port,
    }
    if tags:
        payload["Tags"] = tags
    if check:
        payload["Check"] = check
    if meta:
        payload["Meta"] = meta
    status, _ = _api("PUT", "/agent/service/register", body=payload)
    return status == 200


def deregister_service(service_id: str) -> None:
    """Deregister a service from Consul."""
    _api("PUT", f"/agent/service/deregister/{service_id}")


def list_services() -> dict[str, Any]:
    """Return all services registered with the local agent."""
    _, data = _api("GET", "/agent/services")
    return data or {}


# --------------------------------------------------------------------------- #
# Health Checks
# --------------------------------------------------------------------------- #

def register_check(
    check_id: str,
    name: str,
    health_check_type: str,
    service_id: Optional[str] = None,
    http_url: Optional[str] = None,
    tcp: Optional[str] = None,
    interval: str = "10s",
    timeout: str = "5s",
    notes: str = "",
) -> bool:
    """
    Register a health check.

    Args:
        check_id: Unique check ID.
        name: Human-readable name.
        health_check_type: 'http', 'tcp', 'ttl', or 'script'.
        service_id: Associate with a specific service.
        http_url: URL for HTTP health check.
        tcp: Host:port for TCP health check.
        interval: Check interval (e.g. '10s').
        timeout: Check timeout.
        notes: Additional notes.

    Returns:
        True if registration succeeded.
    """
    payload: dict[str, Any] = {
        "ID": check_id,
        "Name": name,
        "Interval": interval,
        "Timeout": timeout,
        "Notes": notes,
    }
    if health_check_type == "http" and http_url:
        payload["HTTP"] = http_url
    elif health_check_type == "tcp" and tcp:
        payload["TCP"] = tcp
    elif health_check_type == "ttl":
        payload["TTL"] = "30s"
    if service_id:
        payload["ServiceID"] = service_id
    status, _ = _api("PUT", "/agent/check/register", body=payload)
    return status == 200


def pass_check(check_id: str, note: str = "") -> None:
    """Send a pass heartbeat for a check."""
    path = f"/agent/check/pass/{check_id}"
    if note:
        _, _ = _api("PUT", path, body={"Note": note})
    else:
        _, _ = _api("PUT", path)


def fail_check(check_id: str, note: str = "") -> None:
    """Mark a check as failing."""
    path = f"/agent/check/fail/{check_id}"
    if note:
        _, _ = _api("PUT", path, body={"Note": note})
    else:
        _, _ = _api("PUT", path)


# --------------------------------------------------------------------------- #
# Health Endpoints
# --------------------------------------------------------------------------- #

def health_service(
    name: str,
    passing_only: bool = True,
) -> list[dict[str, Any]]:
    """
    Return healthy instances of a service.

    Args:
        name: Service name.
        passing_only: Return only passing checks.

    Returns:
        List of service instance dicts.
    """
    _, data = _api(
        "GET",
        f"/health/service/{name}",
        params={"passing": "1"} if passing_only else None,
    )
    return data or []


def health_checks(service_name: str) -> list[dict[str, Any]]:
    """Return all checks for a service."""
    _, data = _api("GET", f"/health/service/{service_name}")
    return data or []


# --------------------------------------------------------------------------- #
# KV Store
# --------------------------------------------------------------------------- #

def kv_get(
    key: str,
    recurse: bool = False,
    raw: bool = False,
) -> Any:
    """
    Read from the KV store.

    Args:
        key: Key path.
        recurse: List all keys under this path.
        raw: Return raw bytes.

    Returns:
        Value or list of key/value pairs.
    """
    params: dict[str, str] = {}
    if recurse:
        params["recurse"] = "1"
    if raw:
        params["raw"] = "1"
    status, data = _api("GET", f"/kv/{key}", params=params)
    if status == 404:
        return None
    return data


def kv_put(key: str, value: str) -> bool:
    """
    Write a value to the KV store.

    Args:
        key: Key path.
        value: String value to store.

    Returns:
        True if the write succeeded.
    """
    data = value.encode() if isinstance(value, str) else value
    req = urllib.request.Request(
        f"{CONSUL_API_BASE}/kv/{key}",
        data=data,
        headers=_headers(),
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == True
    except Exception:
        return False


def kv_delete(key: str, recurse: bool = False) -> None:
    """Delete a key or all keys under a path."""
    params: dict[str, str] = {}
    if recurse:
        params["recurse"] = "1"
    _api("DELETE", f"/kv/{key}", params=params)


# --------------------------------------------------------------------------- #
# Sessions
# --------------------------------------------------------------------------- #

def create_session(
    name: str = "",
    ttl: str = "30s",
    node: Optional[str] = None,
) -> str:
    """
    Create a new Consul session.

    Returns:
        Session ID string.
    """
    payload: dict[str, Any] = {"Name": name, "TTL": ttl}
    if node:
        payload["Node"] = node
    _, data = _api("POST", "/session/create", body=payload)
    return data.get("ID", "")


def destroy_session(session_id: str) -> None:
    """Destroy a session."""
    _api("PUT", f"/session/destroy/{session_id}")


def session_node(node: str) -> list[dict[str, Any]]:
    """Return all sessions associated with a node."""
    _, data = _api("GET", f"/session/node/{node}")
    return data or []
