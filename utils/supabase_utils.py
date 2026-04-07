"""
Supabase Backend Utilities.

Helpers for managing Supabase projects, databases, auth, storage,
and invoking Edge Functions via the Supabase platform API.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

SUPABASE_ACCESS_TOKEN = os.getenv("SUPABASE_ACCESS_TOKEN", "")
SUPABASE_REF = os.getenv("SUPABASE_REF", "")  # Project ref ID
SUPABASE_API_BASE = "https://api.supabase.com/v1"


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {SUPABASE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
        "apikey": SUPABASE_ACCESS_TOKEN,
    }


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{SUPABASE_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise SupabaseAPIError(exc.code, exc.read().decode()) from exc


class SupabaseAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Supabase API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Projects
# --------------------------------------------------------------------------- #

def list_projects() -> list[dict[str, Any]]:
    """Return all Supabase projects for the account."""
    return _api("GET", "/projects") or []


def get_project(ref: Optional[str] = None) -> dict[str, Any]:
    """Fetch project details."""
    rid = ref or SUPABASE_REF
    if not rid:
        raise ValueError("No SUPABASE_REF set")
    return _api("GET", f"/projects/{rid}")


# --------------------------------------------------------------------------- #
# Database Management
# --------------------------------------------------------------------------- #

def run_sql(
    query: str,
    ref: Optional[str] = None,
) -> dict[str, Any]:
    """
    Execute a raw SQL query against the Postgres database.

    Args:
        query: SQL query string.
        ref: Override project ref.

    Returns:
        Query result with rows and count.
    """
    rid = ref or SUPABASE_REF
    return _api(
        "POST",
        f"/projects/{rid}/database/query",
        body={"query": query},
    )


def list_tables(ref: Optional[str] = None) -> list[str]:
    """
    List all table names in the public schema.

    Returns:
        List of table name strings.
    """
    rid = ref or SUPABASE_REF
    result = run_sql(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' ORDER BY table_name",
        ref=rid,
    )
    return [row.get("table_name", "") for row in result.get("rows", [])]


def get_table_schema(
    table: str,
    ref: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Return column definitions for a table."""
    rid = ref or SUPABASE_REF
    result = run_sql(
        f"SELECT column_name, data_type, is_nullable, column_default "
        f"FROM information_schema.columns "
        f"WHERE table_name = '{table}' AND table_schema = 'public' "
        f"ORDER BY ordinal_position",
        ref=rid,
    )
    return result.get("rows", [])


# --------------------------------------------------------------------------- #
# Auth
# --------------------------------------------------------------------------- #

def list_users(ref: Optional[str] = None) -> dict[str, Any]:
    """Return all users for a project (admin API)."""
    rid = ref or SUPABASE_REF
    return _api("GET", f"/projects/{rid}/auth/admin/users")


def create_user(
    email: str,
    password: str,
    user_metadata: Optional[dict[str, Any]] = None,
    ref: Optional[str] = None,
) -> dict[str, Any]:
    """Create a new user with email/password."""
    rid = ref or SUPABASE_REF
    payload: dict[str, Any] = {
        "email": email,
        "password": password,
    }
    if user_metadata:
        payload["user_metadata"] = user_metadata
    return _api(
        "POST",
        f"/projects/{rid}/auth/admin/users",
        body=payload,
    )


def delete_user(user_id: str, ref: Optional[str] = None) -> dict[str, Any]:
    """Delete a user by their UUID."""
    rid = ref or SUPABASE_REF
    return _api("DELETE", f"/projects/{rid}/auth/admin/users/{user_id}")


# --------------------------------------------------------------------------- #
# Storage
# --------------------------------------------------------------------------- #

def list_storage_buckets(ref: Optional[str] = None) -> list[dict[str, Any]]:
    """List all storage buckets."""
    rid = ref or SUPABASE_REF
    return _api("GET", f"/projects/{rid}/storage/buckets") or []


def create_bucket(
    name: str,
    public: bool = False,
    ref: Optional[str] = None,
) -> dict[str, Any]:
    """Create a new storage bucket."""
    rid = ref or SUPABASE_REF
    return _api(
        "POST",
        f"/projects/{rid}/storage/buckets",
        body={"name": name, "public": public},
    )


def upload_file(
    bucket: str,
    path: str,
    content: bytes,
    content_type: str = "application/octet-stream",
    ref: Optional[str] = None,
) -> dict[str, Any]:
    """Upload a file to a storage bucket."""
    rid = ref or SUPABASE_REF
    import base64
    encoded = base64.b64encode(content).decode()
    return _api(
        "POST",
        f"/projects/{rid}/storage/object/{bucket}/{path}",
        body={
            "bucketId": bucket,
            "name": path,
            "file": encoded,
            "contentType": content_type,
        },
    )


# --------------------------------------------------------------------------- #
# Edge Functions
# --------------------------------------------------------------------------- #

def invoke_edge_function(
    name: str,
    args: Optional[dict[str, Any]] = None,
    ref: Optional[str] = None,
) -> dict[str, Any]:
    """
    Invoke a Supabase Edge Function.

    Args:
        name: Edge Function name.
        args: JSON-serializable arguments passed as request body.
        ref: Override project ref.

    Returns:
        Function response body.
    """
    rid = ref or SUPABASE_REF
    # Note: In production, you should use the JWT from the service role key
    # This uses the Management API for demonstration; for user-level calls
    # use the Supabase JS client with the anon key.
    url = f"https://{rid}.supabase.co/functions/v1/{name}"
    data = json.dumps(args or {}).encode()
    headers = {
        "Authorization": f"Bearer {SUPABASE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise SupabaseAPIError(exc.code, exc.read().decode()) from exc


# --------------------------------------------------------------------------- #
# SSL Certificates
# --------------------------------------------------------------------------- #

def get_custom_ssl_cert(ref: Optional[str] = None) -> dict[str, Any]:
    """Fetch custom SSL certificate status for a project."""
    rid = ref or SUPABASE_REF
    return _api("GET", f"/projects/{rid}/ssl-config")
