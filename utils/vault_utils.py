"""
HashiCorp Vault Secrets Utilities.

Helpers for reading, writing, and managing secrets in HashiCorp Vault,
including dynamic credentials, transit encryption, and policy management.

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

VAULT_ADDR = os.getenv("VAULT_ADDR", "http://localhost:8200")
VAULT_TOKEN = os.getenv("VAULT_TOKEN", "")
VAULT_API_BASE = f"{VAULT_ADDR}/v1"


def _headers() -> dict[str, str]:
    h: dict[str, str] = {"Content-Type": "application/json"}
    if VAULT_TOKEN:
        h["X-Vault-Token"] = VAULT_TOKEN
    return h


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
) -> tuple[int, dict[str, Any]]:
    url = f"{VAULT_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return (resp.status, json.loads(resp.read()))
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode()
        raise VaultAPIError(exc.code, body_text) from exc


class VaultAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Vault API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Health & Status
# --------------------------------------------------------------------------- #

def health() -> dict[str, Any]:
    """Return Vault cluster health status."""
    url = f"{VAULT_ADDR}/v1/sys/health"
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return json.loads(exc.read())


def status() -> dict[str, Any]:
    """Return Vault seal status and high availability info."""
    status_code, data = _api("GET", "/sys/status")
    return data


# --------------------------------------------------------------------------- #
# KV Secrets (v2)
# --------------------------------------------------------------------------- #

def kv2_put(
    path: str,
    secret: dict[str, Any],
    mount: str = "secret",
) -> bool:
    """
    Write a secret to KV v2.

    Args:
        path: Secret path (without 'data/' prefix).
        secret: Key-value pairs to store.
        mount: KV mount point (default: 'secret').

    Returns:
        True if write succeeded.
    """
    status, _ = _api(
        "POST",
        f"/{mount}/data/{path}",
        body={"data": secret},
    )
    return status in (200, 201)


def kv2_get(
    path: str,
    mount: str = "secret",
    version: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    """
    Read a secret from KV v2.

    Args:
        path: Secret path.
        mount: KV mount point.
        version: Specific version to read.

    Returns:
        Secret data dict or None if not found.
    """
    params: dict[str, str] = {}
    if version:
        params["version"] = str(version)
    qs = urllib.parse.urlencode(params)
    url_path = f"/{mount}/data/{path}"
    if qs:
        url_path = f"{url_path}?{qs}"
    status, data = _api("GET", url_path)
    if status == 404:
        return None
    return data.get("data", {}).get("data", {})


def kv2_delete(
    path: str,
    mount: str = "secret",
    versions: Optional[list[int]] = None,
) -> None:
    """
    Delete a secret (or specific versions) from KV v2.

    Args:
        path: Secret path.
        mount: KV mount point.
        versions: Specific versions to delete (soft delete).
    """
    if versions:
        body = {"versions": [str(v) for v in versions]}
        _api("POST", f"/{mount}/delete/{path}", body=body)
    else:
        _api("DELETE", f"/{mount}/data/{path}")


def kv2_list(
    path: str = "",
    mount: str = "secret",
) -> list[str]:
    """
    List secret keys at a path.

    Args:
        path: Directory path to list.
        mount: KV mount point.

    Returns:
        List of key names.
    """
    status, data = _api("LIST", f"/{mount}/metadata/{path}")
    if status == 404:
        return []
    return data.get("data", {}).get("keys", [])


# --------------------------------------------------------------------------- #
# Dynamic Secrets (Database)
# --------------------------------------------------------------------------- #

def get_db_credential(
    role: str,
    mount: str = "database",
) -> Optional[dict[str, str]]:
    """
    Generate dynamic database credentials.

    Args:
        role: Database role name configured in Vault.
        mount: Database secrets engine mount path.

    Returns:
        Dict with 'username' and 'password' keys, or None on failure.
    """
    status, data = _api("GET", f"/{mount}/creds/{role}")
    if status in (200, 201):
        return data.get("data", {})
    return None


# --------------------------------------------------------------------------- #
# Transit Encryption
# --------------------------------------------------------------------------- #

def transit_encrypt(
    key_name: str,
    plaintext: str,
    mount: str = "transit",
) -> Optional[str]:
    """
    Encrypt plaintext using the Transit secrets engine.

    Args:
        key_name: Named encryption key.
        plaintext: Base64-encoded plaintext to encrypt.
        mount: Transit mount point.

    Returns:
        Base64-encoded ciphertext, or None on failure.
    """
    import base64
    encoded = base64.b64encode(plaintext.encode()).decode()
    status, data = _api(
        "POST",
        f"/{mount}/encrypt/{key_name}",
        body={"plaintext": encoded},
    )
    if status in (200, 201):
        return data.get("data", {}).get("ciphertext")
    return None


def transit_decrypt(
    key_name: str,
    ciphertext: str,
    mount: str = "transit",
) -> Optional[str]:
    """
    Decrypt ciphertext using the Transit secrets engine.

    Args:
        key_name: Named encryption key.
        ciphertext: Base64-encoded ciphertext from encrypt.
        mount: Transit mount point.

    Returns:
        Decrypted plaintext string, or None on failure.
    """
    import base64
    status, data = _api(
        "POST",
        f"/{mount}/decrypt/{key_name}",
        body={"ciphertext": ciphertext},
    )
    if status in (200, 201):
        encoded = data.get("data", {}).get("plaintext", "")
        return base64.b64decode(encoded).decode()
    return None


# --------------------------------------------------------------------------- #
# Policies
# --------------------------------------------------------------------------- #

def list_policies() -> list[str]:
    """List all named policies."""
    status, data = _api("LIST", "/sys/policies/acl")
    return data.get("policies", [])


def get_policy(name: str) -> Optional[str]:
    """Fetch a policy by name."""
    status, data = _api("GET", f"/sys/policies/acl/{name}")
    if status == 200:
        return data.get("policy", "")
    return None


def put_policy(name: str, rules: str) -> bool:
    """
    Create or update an ACL policy.

    Args:
        name: Policy name.
        rules: HCL policy rules.

    Returns:
        True if update succeeded.
    """
    status, _ = _api(
        "PUT",
        f"/sys/policies/acl/{name}",
        body={"policy": rules},
    )
    return status in (200, 201, 204)


# --------------------------------------------------------------------------- #
# Auth Methods
# --------------------------------------------------------------------------- #

def list_auth_methods() -> list[dict[str, Any]]:
    """List all enabled authentication methods."""
    status, data = _api("GET", "/sys/auth")
    if status == 200:
        return [
            {"path": k, "config": v}
            for k, v in data.get("data", {}).items()
        ]
    return []


# --------------------------------------------------------------------------- #
# Token Management
# --------------------------------------------------------------------------- #

def create_token(
    policies: Optional[list[str]] = None,
    ttl: str = "1h",
    renewable: bool = True,
) -> Optional[dict[str, Any]]:
    """
    Create a new Vault token.

    Args:
        policies: List of policy names to attach.
        ttl: Token TTL (e.g. '1h', '24h').
        renewable: Whether the token can be renewed.

    Returns:
        Token metadata including 'auth' dict with client_token.
    """
    payload: dict[str, Any] = {
        "policies": policies or ["default"],
        "ttl": ttl,
        "renewable": renewable,
    }
    status, data = _api("POST", "/auth/token/create", body=payload)
    if status in (200, 201):
        return data.get("auth", {})
    return None


def renew_token(accessor: str = "") -> dict[str, Any]:
    """Renew a token by its accessor."""
    if accessor:
        return _api("POST", "/auth/token/renew-accessor", body={"accessor": accessor})[1]
    return _api("POST", "/auth/token/renew-self")[1]
