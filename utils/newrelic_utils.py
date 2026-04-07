"""
New Relic APM & Infrastructure Utilities.

Helpers for querying the New Relic API, managing alerts, inserting custom
events, fetching APM metrics, and interacting with the New Relic NerdGraph API.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

NEWRELIC_API_KEY = os.getenv("NEWRELIC_API_KEY", "")
NEWRELIC_ACCOUNT_ID = os.getenv("NEWRELIC_ACCOUNT_ID", "")
NEWRELIC_API_BASE = "https://api.newrelic.com/v2"


def _headers() -> dict[str, str]:
    return {
        "Api-Key": NEWRELIC_API_KEY,
        "Content-Type": "application/json",
    }


def _gql_headers() -> dict[str, str]:
    return {
        "Api-Key": NEWRELIC_API_KEY,
        "Content-Type": "application/json",
    }


def _api_get(path: str, params: Optional[dict[str, str]] = None) -> dict[str, Any]:
    url = f"{NEWRELIC_API_BASE}{path}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{qs}"
    req = urllib.request.Request(url, headers=_headers())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise NewRelicAPIError(exc.code, exc.read().decode()) from exc


def _api_post(path: str, body: dict[str, Any]) -> dict[str, Any]:
    url = f"{NEWRELIC_API_BASE}{path}"
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise NewRelicAPIError(exc.code, exc.read().decode()) from exc


class NewRelicAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"New Relic API error {status}: {body}")


# --------------------------------------------------------------------------- #
# APM Applications
# --------------------------------------------------------------------------- #

def list_applications(
    name: Optional[str] = None,
    ids: Optional[list[int]] = None,
) -> list[dict[str, Any]]:
    """
    List APM applications.

    Args:
        name: Filter by application name.
        ids: Filter by application IDs.

    Returns:
        List of application objects.
    """
    params: dict[str, str] = {}
    if name:
        params["filter[name]"] = name
    if ids:
        params["filter[ids]"] = ",".join(str(i) for i in ids)
    data = _api_get("/applications.json", params=params)
    return data.get("applications", [])


def get_application(app_id: int) -> dict[str, Any]:
    """Fetch a single application by ID."""
    data = _api_get(f"/applications/{app_id}.json")
    return data.get("application", {})


def get_application_metrics(
    app_id: int,
    names: Optional[list[str]] = None,
    duration: int = 30,
) -> dict[str, Any]:
    """
    Fetch metric data for an application.

    Args:
        app_id: Application ID.
        names: List of metric names to retrieve.
        duration: Time window in minutes.

    Returns:
        Metric data dict.
    """
    params: dict[str, str] = {"period": str(duration)}
    if names:
        params["names"] = ",".join(names)
    return _api_get(f"/applications/{app_id}/metrics.json", params=params)


# --------------------------------------------------------------------------- #
# Alert Policies & Incidents
# --------------------------------------------------------------------------- #

def list_alert_policies() -> list[dict[str, Any]]:
    """Return all alert policies for the account."""
    if not NEWRELIC_ACCOUNT_ID:
        raise ValueError("NEWRELIC_ACCOUNT_ID not set")
    data = _api_get(
        f"/alerts_policies.json",
        params={"account_id": NEWRELIC_ACCOUNT_ID},
    )
    return data.get("policies", [])


def create_alert_policy(name: str, incident_preference: str = "PER_POLICY") -> dict[str, Any]:
    """Create a new alert policy."""
    if not NEWRELIC_ACCOUNT_ID:
        raise ValueError("NEWRELIC_ACCOUNT_ID not set")
    return _api_post(
        "/alerts_policies.json",
        body={
            "policy": {
                "name": name,
                "incident_preference": incident_preference,
                "account_id": int(NEWRELIC_ACCOUNT_ID),
            }
        },
    )


def list_alerts(account_id: Optional[str] = None) -> list[dict[str, Any]]:
    """List open alerts (incidents) for an account."""
    aid = account_id or NEWRELIC_ACCOUNT_ID
    return _api_get(f"/alerts_incidents.json", params={"account_id": aid})


def close_incident(incident_id: int) -> dict[str, Any]:
    """Close a New Relic alert incident."""
    return _api_post(
        "/alerts_incidents.json",
        body={"incident": {"id": incident_id, "status": "CLOSED"}},
    )


# --------------------------------------------------------------------------- #
# Custom Events (Insights API)
# --------------------------------------------------------------------------- #

def insert_custom_event(
    event_type: str,
    attributes: dict[str, Any],
) -> dict[str, Any]:
    """
    Insert a custom event into New Relic Insights.

    Args:
        event_type: Name of the event type.
        attributes: Key-value attributes for the event.

    Returns:
        API response dict.
    """
    INSIGHTS_API = "https://insights-api.newrelic.com/v1/accounts"
    if not NEWRELIC_ACCOUNT_ID:
        raise ValueError("NEWRELIC_ACCOUNT_ID not set")
    url = f"{INSIGHTS_API}/{NEWRELIC_ACCOUNT_ID}/events"
    data = json.dumps([{"eventType": event_type, **attributes}]).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Api-Key": NEWRELIC_API_KEY,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return {"status": resp.status}
    except urllib.error.HTTPError as exc:
        raise NewRelicAPIError(exc.code, exc.read().decode()) from exc


# --------------------------------------------------------------------------- #
# NerdGraph (GraphQL API) — generic query/mutation
# --------------------------------------------------------------------------- #

NERDGRAPH_URL = "https://api.newrelic.com/graphql"


def nerdgraph_query(
    query: str,
    variables: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Execute a NerdGraph GraphQL query.

    Args:
        query: GraphQL query string.
        variables: Optional query variables.

    Returns:
        Parsed GraphQL response data.
    """
    payload: dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        NERDGRAPH_URL,
        data=data,
        headers=_gql_headers(),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            if "errors" in result:
                raise NewRelicAPIError(400, str(result["errors"]))
            return result.get("data", {})
    except urllib.error.HTTPError as exc:
        raise NewRelicAPIError(exc.code, exc.read().decode()) from exc


def get_entity_guid(name: str, domain: str = "APM") -> Optional[str]:
    """
    Look up a New Relic entity GUID by name.

    Returns:
        Entity GUID string or None if not found.
    """
    gql = """
    query($name: String!, $domain: EntityDomain!) {
        actor {
            entitySearch(query: "name = '%s' and domain = '%s'") {
                results { entities { guid name } }
            }
        }
    }
    """ % (name, domain)
    result = nerdgraph_query(gql)
    entities = (
        result.get("actor", {})
        .get("entitySearch", {})
        .get("results", {})
        .get("entities", [])
    )
    return entities[0].get("guid") if entities else None


# --------------------------------------------------------------------------- #
# Infrastructure
# --------------------------------------------------------------------------- #

def list_infrastructure_hosts(
    filters: Optional[dict[str, str]] = None,
) -> list[dict[str, Any]]:
    """List infrastructure monitoring hosts."""
    params: dict[str, str] = {}
    if filters:
        for k, v in filters.items():
            params[f"filter[{k}]"] = v
    return _api_get("/servers.json", params=params) or []


def get_server(server_id: int) -> dict[str, Any]:
    """Fetch a single server/host by ID."""
    return _api_get(f"/servers/{server_id}.json")
