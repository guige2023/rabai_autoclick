"""
Railway Deployment Utilities.

Helpers for deploying and managing applications on Railway via the Railway API.

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

RAILWAY_TOKEN = os.getenv("RAILWAY_TOKEN", "")
RAILWAY_API_BASE = "https://backboard.railway.app/graphql"


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {RAILWAY_TOKEN}",
        "Content-Type": "application/json",
    }


def _gql(query: str, variables: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """Execute a GraphQL query against the Railway API."""
    payload: dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        RAILWAY_API_BASE,
        data=data,
        headers=_headers(),
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            if "errors" in result:
                raise RailwayAPIError(result["errors"])
            return result.get("data", {})
    except urllib.error.HTTPError as exc:
        raise RailwayAPIError([{"message": exc.read().decode()}]) from exc


class RailwayAPIError(Exception):
    def __init__(self, errors: list[dict[str, str]]) -> None:
        msgs = "; ".join(e.get("message", str(e)) for e in errors)
        super().__init__(f"Railway API error: {msgs}")


# --------------------------------------------------------------------------- #
# Projects
# --------------------------------------------------------------------------- #

def list_projects() -> list[dict[str, Any]]:
    """Return all Railway projects."""
    data = _gql("""
    {
        projects {
            id
            name
            description
            createdAt
        }
    }
    """)
    return data.get("projects", [])


def get_project(project_id: str) -> dict[str, Any]:
    """Fetch a project by ID."""
    data = _gql(
        """
        query GetProject($id: String!) {
            project(id: $id) { id name description }
        }
        """,
        {"id": project_id},
    )
    return data.get("project", {})


def create_project(name: str, description: str = "") -> dict[str, Any]:
    """Create a new Railway project."""
    data = _gql(
        """
        mutation CreateProject($name: String!, $description: String) {
            projectCreate(input: { name: $name, description: $description }) {
                id name
            }
        }
        """,
        {"name": name, "description": description},
    )
    return data.get("projectCreate", {})


# --------------------------------------------------------------------------- #
# Environments
# --------------------------------------------------------------------------- #

def list_environments(project_id: str) -> list[dict[str, Any]]:
    """Return environments (production, preview, etc.) for a project."""
    data = _gql(
        """
        query GetEnvironments($projectId: String!) {
            environments(projectId: $projectId) {
                id name slug isDefault
            }
        }
        """,
        {"projectId": project_id},
    )
    return data.get("environments", [])


# --------------------------------------------------------------------------- #
# Services
# --------------------------------------------------------------------------- #

def list_services(project_id: str) -> list[dict[str, Any]]:
    """List all services in a project."""
    data = _gql(
        """
        query GetServices($projectId: String!) {
            services(projectId: $projectId) {
                id name
            }
        }
        """,
        {"projectId": project_id},
    )
    return data.get("services", [])


def get_service(service_id: str) -> dict[str, Any]:
    """Fetch a service by ID."""
    data = _gql(
        """
        query GetService($id: String!) {
            service(id: $id) { id name description }
        }
        """,
        {"id": service_id},
    )
    return data.get("service", {})


# --------------------------------------------------------------------------- #
# Deployments
# --------------------------------------------------------------------------- #

def list_deployments(
    project_id: str,
    environment_id: Optional[str] = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """List recent deployments."""
    data = _gql(
        """
        query GetDeployments($projectId: String!, $environmentId: String, $limit: Int) {
            deployments(projectId: $projectId, environmentId: $environmentId, limit: $limit) {
                id
                status
                createdAt
                updatedAt
            }
        }
        """,
        {"projectId": project_id, "environmentId": environment_id, "limit": limit},
    )
    return data.get("deployments", [])


def redeploy(service_id: str, environment_id: str) -> dict[str, Any]:
    """Trigger a new deployment for a service in an environment."""
    data = _gql(
        """
        mutation Redeploy($serviceId: String!, $environmentId: String!) {
            deploymentRedploy(serviceId: $serviceId, environmentId: $environmentId) {
                id status
            }
        }
        """,
        {"serviceId": service_id, "environmentId": environment_id},
    )
    return data.get("deploymentRedploy", {})


# --------------------------------------------------------------------------- #
# Environment Variables
# --------------------------------------------------------------------------- #

def get_variables(
    service_id: Optional[str],
    environment_id: str,
) -> list[dict[str, Any]]:
    """Fetch environment variables for a service or project."""
    service_arg = f'serviceId: "{service_id}"' if service_id else ""
    query = f"""
    {{
        variables(serviceId: "{service_id}", environmentId: "{environment_id}") {{
            key value
        }}
    }}
    """
    data = _gql(query)
    return data.get("variables", [])


def set_variable(
    service_id: Optional[str],
    environment_id: str,
    key: str,
    value: str,
) -> dict[str, Any]:
    """Set an environment variable."""
    data = _gql(
        """
        mutation SetVariable($serviceId: String, $environmentId: String!, $key: String!, $value: String!) {
            variableCreate(input: { serviceId: $serviceId, environmentId: $environmentId, key: $key, value: $value }) {
                key value
            }
        }
        """,
        {
            "serviceId": service_id,
            "environmentId": environment_id,
            "key": key,
            "value": value,
        },
    )
    return data.get("variableCreate", {})


def delete_variable(
    service_id: Optional[str],
    environment_id: str,
    key: str,
) -> None:
    """Delete an environment variable."""
    _gql(
        """
        mutation DeleteVariable($serviceId: String, $environmentId: String!, $key: String!) {
            variableDelete(serviceId: $serviceId, environmentId: $environmentId, key: $key)
        }
        """,
        {"serviceId": service_id, "environmentId": environment_id, "key": key},
    )
