"""
ArgoCD GitOps Utilities.

Helpers for managing ArgoCD applications, projects, sync waves,
and interacting with the ArgoCD API for GitOps workflows.

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

ARGOCD_SERVER = os.getenv("ARGOCD_SERVER", "localhost:8080")
ARGOCD_TOKEN = os.getenv("ARGOCD_TOKEN", "")
ARGOCD_API_BASE = f"https://{ARGOCD_SERVER}/api/v1"


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {ARGOCD_TOKEN}",
        "Content-Type": "application/json",
    }


def _api(
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    url = f"{ARGOCD_API_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, headers=_headers(), method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise ArgoCDAPIError(exc.code, exc.read().decode()) from exc


class ArgoCDAPIError(Exception):
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body
        super().__init__(f"ArgoCD API error {status}: {body}")


# --------------------------------------------------------------------------- #
# Applications
# --------------------------------------------------------------------------- #

def list_applications() -> list[dict[str, Any]]:
    """Return all ArgoCD applications."""
    return _api("GET", "/applications") or []


def get_application(name: str) -> dict[str, Any]:
    """Fetch a single application by name."""
    return _api("GET", f"/applications/{name}")


def create_application(
    name: str,
    repo_url: str,
    path: str,
    target_revision: str = "HEAD",
    namespace: str = "default",
    server: str = "https://kubernetes.default.svc",
    project: str = "default",
    sync_policy: Optional[str] = None,
    auto_sync: bool = False,
) -> dict[str, Any]:
    """
    Create a new ArgoCD application.

    Args:
        name: Application name.
        repo_url: Git repository URL.
        path: Path within the repository.
        target_revision: Git revision (branch, tag, or SHA).
        namespace: Target Kubernetes namespace.
        server: Target cluster server URL.
        project: ArgoCD project name.
        sync_policy: 'manual' or 'auto'.
        auto_sync: Enable automated sync.

    Returns:
        Created application object.
    """
    spec: dict[str, Any] = {
        "source": {
            "repoURL": repo_url,
            "path": path,
            "targetRevision": target_revision,
        },
        "destination": {
            "server": server,
            "namespace": namespace,
        },
        "project": project,
        "syncPolicy": {},
    }
    if sync_policy:
        spec["syncPolicy"]["automated"] = {"enabled": True} if sync_policy == "auto" else None
    if auto_sync:
        spec["syncPolicy"]["automated"] = {"enabled": True}
    payload: dict[str, Any] = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Application",
        "metadata": {"name": name},
        "spec": spec,
    }
    return _api("POST", "/applications", body=payload)


def sync_application(
    name: str,
    revision: Optional[str] = None,
    prune: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Trigger a sync for an application.

    Args:
        name: Application name.
        revision: Optional specific revision to sync.
        prune: Include pruning of deleted resources.
        dry_run: Dry-run sync without applying changes.

    Returns:
        Sync operation result.
    """
    body: dict[str, Any] = {
        "revision": revision or "",
        "prune": prune,
        "dryRun": dry_run,
    }
    return _api("POST", f"/applications/{name}/sync", body=body)


def delete_application(name: str) -> None:
    """Delete an ArgoCD application."""
    _api("DELETE", f"/applications/{name}")


# --------------------------------------------------------------------------- #
# Application Health & Status
# --------------------------------------------------------------------------- #

def get_application_status(name: str) -> dict[str, Any]:
    """Return the health and sync status of an application."""
    app = get_application(name)
    return {
        "health": app.get("status", {}).get("health", {}),
        "sync": app.get("status", {}).get("sync", {}),
        "history": app.get("status", {}).get("history", []),
        "conditions": app.get("status", {}).get("conditions", []),
    }


def get_application_history(name: str) -> list[dict[str, Any]]:
    """Return deployment history for an application."""
    app = get_application(name)
    return app.get("status", {}).get("history", [])


def rollback_application(name: str, revision: str) -> dict[str, Any]:
    """Rollback an application to a specific revision."""
    return _api("POST", f"/applications/{name}/rollback", body={"revision": revision})


# --------------------------------------------------------------------------- #
# Projects
# --------------------------------------------------------------------------- #

def list_projects() -> list[dict[str, Any]]:
    """List all ArgoCD projects."""
    return _api("GET", "/projects") or []


def get_project(project: str) -> dict[str, Any]:
    """Fetch a project by name."""
    return _api("GET", f"/projects/{project}")


def create_project(
    name: str,
    description: str = "",
    source_repos: Optional[list[str]] = None,
    destinations: Optional[list[dict[str, str]]] = None,
) -> dict[str, Any]:
    """
    Create an ArgoCD project.

    Args:
        name: Project name.
        description: Project description.
        source_repos: Allowed source repository globs.
        destinations: Allowed destination clusters/namespaces.
    """
    payload: dict[str, Any] = {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "AppProject",
        "metadata": {"name": name},
        "spec": {
            "sourceRepos": source_repos or ["*"],
            "destinations": destinations or [
                {"server": "*", "namespace": "*"}
            ],
            "description": description,
        },
    }
    return _api("POST", "/projects", body=payload)


# --------------------------------------------------------------------------- #
# Cluster Management
# --------------------------------------------------------------------------- #

def list_clusters() -> list[dict[str, Any]]:
    """List all registered clusters."""
    return _api("GET", "/clusters") or []


def get_cluster(server: str) -> dict[str, Any]:
    """Fetch cluster info by server URL."""
    return _api("GET", f"/clusters/{server}")


# --------------------------------------------------------------------------- #
# Resource Tree
# --------------------------------------------------------------------------- #

def get_resource_tree(name: str) -> dict[str, Any]:
    """Get the full resource tree for an application."""
    return _api("GET", f"/applications/{name}/resource-tree")


def get_resource_status(
    name: str,
    group: str,
    kind: str,
    namespace: str,
    name_: str,
) -> dict[str, Any]:
    """Fetch detailed status for a specific managed resource."""
    path = (
        f"/applications/{name}/resource"
        f"?group={group}&kind={kind}&namespace={namespace}&name={name_}"
    )
    return _api("GET", path)
