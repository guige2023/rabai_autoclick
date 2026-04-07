"""
Elasticsearch & ELK Stack Utilities.

Helpers for querying, indexing, and managing Elasticsearch indices,
aggregations, mapping, cluster health, and document lifecycle management.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "")
ES_PASSWORD = os.getenv("ES_PASSWORD", "")
ES_API_BASE = ES_HOST.rstrip("/")


def _headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    if ES_USER and ES_PASSWORD:
        import base64
        creds = base64.b64encode(f"{ES_USER}:{ES_PASSWORD}".encode()).decode()
        h["Authorization"] = f"Basic {creds}"
    return h


def _request(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{ES_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise ElasticsearchAPIError(exc.code, exc.read().decode()) from exc


class ElasticsearchAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"Elasticsearch API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Cluster Health
# --------------------------------------------------------------------------- #

def cluster_health() -> dict[str, Any]:
    """Return cluster health status."""
    return _request("GET", "/_cluster/health")


def cluster_stats() -> dict[str, Any]:
    """Return detailed cluster statistics."""
    return _request("GET", "/_cluster/stats")


def nodes_stats() -> dict[str, Any]:
    """Return statistics for all nodes."""
    return _request("GET", "/_nodes/stats")


# --------------------------------------------------------------------------- #
# Indices
# --------------------------------------------------------------------------- #

def list_indices() -> list[dict[str, Any]]:
    """Return all indices with their metadata."""
    data = _request("GET", "/_cat/indices?format=json")
    return data if isinstance(data, list) else []


def create_index(
    index: str,
    mappings: Optional[dict[str, Any]] = None,
    settings: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Create an index with optional mappings and settings.

    Args:
        index: Index name.
        mappings: Field mappings dict.
        settings: Index settings (shards, replicas, etc.).
    """
    body: dict[str, Any] = {}
    if mappings:
        body["mappings"] = mappings
    if settings:
        body["settings"] = settings
    return _request("PUT", f"/{index}", body=body if body else None)


def delete_index(index: str) -> dict[str, Any]:
    """Delete an index."""
    return _request("DELETE", f"/{index}")


def index_exists(index: str) -> bool:
    """Check if an index exists."""
    url = f"{ES_API_BASE}/{index}"
    req = urllib.request.Request(url, headers=_headers(), method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except urllib.error.HTTPError:
        return False


def get_mapping(index: str) -> dict[str, Any]:
    """Get field mappings for an index."""
    return _request("GET", f"/{index}/_mapping")


# --------------------------------------------------------------------------- #
# Documents
# --------------------------------------------------------------------------- #

def index_document(
    index: str,
    doc_id: Optional[str],
    document: dict[str, Any],
    refresh: bool = False,
) -> dict[str, Any]:
    """
    Index a single document.

    Args:
        index: Target index.
        doc_id: Document ID (optional — auto-generated if omitted).
        document: Document body.
        refresh: If True, refresh the index immediately.

    Returns:
        Index result with _id and _index.
    """
    path = f"/{index}/_doc"
    if doc_id:
        path = f"/{index}/_doc/{doc_id}"
    params = {"refresh": "true" if refresh else "false"}
    qs = urllib.parse.urlencode(params)
    return _request("PUT", f"{path}?{qs}", body=document)


def get_document(index: str, doc_id: str) -> dict[str, Any]:
    """Retrieve a document by ID."""
    return _request("GET", f"/{index}/_doc/{doc_id}")


def search(
    index: str,
    query: Optional[dict[str, Any]] = None,
    size: int = 10,
    from_: int = 0,
    sort: Optional[list[dict[str, Any]]] = None,
    aggregations: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Execute a search query.

    Args:
        index: Target index (or comma-separated list).
        query: ES query DSL dict.
        size: Number of results.
        from_: Offset for pagination.
        sort: Sort specification.
        aggregations: Aggregation definitions.

    Returns:
        Search response with hits and optional aggregations.
    """
    body: dict[str, Any] = {
        "query": query or {"match_all": {}},
        "size": size,
        "from": from_,
    }
    if sort:
        body["sort"] = sort
    if aggregations:
        body["aggs"] = aggregations
    return _request("POST", f"/{index}/_search", body=body)


def delete_document(index: str, doc_id: str, refresh: bool = False) -> dict[str, Any]:
    """Delete a document by ID."""
    params = {"refresh": "true" if refresh else "false"}
    qs = urllib.parse.urlencode(params)
    return _request("DELETE", f"/{index}/_doc/{doc_id}?{qs}")


# --------------------------------------------------------------------------- #
# Bulk Operations
# --------------------------------------------------------------------------- #

def bulk_index(
    index: str,
    documents: list[dict[str, Any]],
    id_field: str = "id",
    refresh: bool = False,
) -> dict[str, Any]:
    """
    Bulk index documents using NDJSON format.

    Args:
        index: Target index.
        documents: List of document dicts.
        id_field: Field to use as the document ID.
        refresh: If True, refresh the index after bulk indexing.

    Returns:
        Bulk API response with success/failure counts.
    """
    lines: list[bytes] = []
    for doc in documents:
        doc_id = doc.get(id_field)
        meta = {"index": {"_index": index}}
        if doc_id:
            meta["index"]["_id"] = str(doc_id)
        lines.append(json.dumps(meta).encode())
        lines.append(json.dumps(doc).encode())
    data = b"\n".join(lines) + b"\n"
    params = {"refresh": "true" if refresh else "false"}
    qs = urllib.parse.urlencode(params)
    url = f"{ES_API_BASE}/_bulk?{qs}"
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise ElasticsearchAPIError(exc.code, exc.read().decode()) from exc


# --------------------------------------------------------------------------- #
# Aggregations
# --------------------------------------------------------------------------- #

def terms_agg(
    index: str,
    field: str,
    size: int = 10,
    query: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Run a terms aggregation on a field."""
    body: dict[str, Any] = {
        "query": query or {"match_all": {}},
        "aggs": {
            "terms": {
                "terms": {"field": field, "size": size}
            }
        },
        "size": 0,
    }
    return _request("POST", f"/{index}/_search", body=body)


def date_histogram_agg(
    index: str,
    field: str,
    calendar_interval: str = "day",
    query: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Run a date histogram aggregation."""
    body: dict[str, Any] = {
        "query": query or {"match_all": {}},
        "aggs": {
            "histogram": {
                "date_histogram": {
                    "field": field,
                    "calendar_interval": calendar_interval,
                }
            }
        },
        "size": 0,
    }
    return _request("POST", f"/{index}/_search", body=body)


# --------------------------------------------------------------------------- #
# Aliases
# --------------------------------------------------------------------------- #

def add_alias(index: str, alias: str) -> dict[str, Any]:
    """Add an alias to an index."""
    return _request("POST", f"/_aliases", body={"actions": [{"add": {"index": index, "alias": alias}}]})


def list_aliases(index: str) -> dict[str, Any]:
    """List all aliases for an index."""
    return _request("GET", f"/{index}/_alias")
