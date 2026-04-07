"""
Helm Chart Utilities.

Helpers for rendering Helm templates, managing releases,
interacting with the Helm API/CLI, and managing chart
repositories for Kubernetes package management.

Author: rabai_autoclick
License: MIT
"""

import os
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, Any


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

HELM_PATH = os.getenv("HELM_PATH", "helm")
KUBE_CONFIG = os.getenv("KUBE_CONFIG", "")
KUBE_CONTEXT = os.getenv("KUBE_CONTEXT", "")


def _kube_args() -> list[str]:
    args: list[str] = []
    if KUBE_CONFIG:
        args += ["--kubeconfig", KUBE_CONFIG]
    if KUBE_CONTEXT:
        args += ["--kube-context", KUBE_CONTEXT]
    return args


def _run(args: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(
        [HELM_PATH] + args,
        capture_output=True,
        text=True,
        **kwargs,
    )


# --------------------------------------------------------------------------- #
# Repositories
# --------------------------------------------------------------------------- #

def add_repo(name: str, url: str, update: bool = True) -> None:
    """
    Add a Helm repository.

    Args:
        name: Repository name.
        url: Repository URL.
        update: Run 'helm repo update' after adding.
    """
    _run(["repo", "add", name, url])
    if update:
        _run(["repo", "update"])


def update_repos() -> None:
    """Update all Helm repositories."""
    _run(["repo", "update"])


def list_repos() -> list[dict[str, str]]:
    """Return installed Helm repositories."""
    result = _run(["repo", "list", "-o", "json"])
    if result.stdout.strip():
        return json.loads(result.stdout)
    return []


def search_repo(
    query: str,
    repo: Optional[str] = None,
    versions: bool = False,
) -> list[dict[str, Any]]:
    """
    Search Helm Hub or a specific repo for charts.

    Args:
        query: Chart name to search.
        repo: Limit search to a specific repository.
        versions: Return all versions.

    Returns:
        List of matching chart info dicts.
    """
    args = ["search", "repo", query]
    if repo:
        args += ["--repo", repo]
    if versions:
        args.append("--versions")
    args += ["-o", "json"]
    result = _run(args)
    if result.stdout.strip():
        return json.loads(result.stdout)
    return []


# --------------------------------------------------------------------------- #
# Chart Rendering
# --------------------------------------------------------------------------- #

def template(
    release_name: str,
    chart: str,
    values: Optional[dict[str, Any]] = None,
    set_values: Optional[dict[str, str]] = None,
    namespace: str = "default",
    validate: bool = True,
) -> str:
    """
    Render Helm templates locally without installing.

    Args:
        release_name: Name for the release.
        chart: Chart reference (local path or repo/chart).
        values: values.yaml overrides as a dict.
        set_values: --set key=value overrides.
        namespace: Target Kubernetes namespace.
        validate: Run 'helm template --validate'.

    Returns:
        Rendered YAML manifest string.
    """
    args = ["template", release_name, chart, "--namespace", namespace]
    if validate:
        args.append("--validate")
    if values:
        import tempfile, json
        with tempfile.NamedTemporaryFile(
            suffix=".yaml", mode="w", delete=False
        ) as f:
            import yaml
            yaml.safe_dump(values, f)
            args += ["--values", f.name]
    if set_values:
        for k, v in set_values.items():
            args += ["--set", f"{k}={v}"]
    result = _run(args)
    if result.returncode != 0:
        raise HelmError(f"Template failed: {result.stderr}")
    return result.stdout


def lint(chart: str) -> dict[str, Any]:
    """
    Lint a Helm chart for issues.

    Returns:
        Lint result with 'passed', 'warnings', and 'errors' fields.
    """
    result = _run(["lint", chart])
    return {
        "passed": result.returncode == 0,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


# --------------------------------------------------------------------------- #
# Releases
# --------------------------------------------------------------------------- #

def list_releases(
    namespace: Optional[str] = None,
    all_namespaces: bool = False,
    status: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    List Helm releases.

    Args:
        namespace: Filter by namespace.
        all_namespaces: List across all namespaces.
        status: Filter by status (deployed, failed, pending, etc.).

    Returns:
        List of release objects.
    """
    args = ["list", "-a"] if all_namespaces else ["list"]
    if namespace:
        args += ["--namespace", namespace]
    if status:
        args += ["--status", status]
    args += ["-o", "json"]
    result = _run(args)
    if result.stdout.strip():
        return json.loads(result.stdout)
    return []


def get_release(
    name: str,
    namespace: Optional[str] = None,
) -> dict[str, Any]:
    """Fetch detailed release metadata."""
    args = ["get", "all", name]
    if namespace:
        args += ["--namespace", namespace]
    args += _kube_args()
    result = _run(args)
    return {"name": name, "stdout": result.stdout, "stderr": result.stderr}


def install_release(
    name: str,
    chart: str,
    namespace: Optional[str] = None,
    values: Optional[dict[str, Any]] = None,
    set_values: Optional[dict[str, str]] = None,
    atomic: bool = False,
    wait: bool = False,
    timeout: str = "5m",
) -> subprocess.CompletedProcess:
    """
    Install a Helm release.

    Returns:
        CompletedProcess with stdout/stderr.
    """
    args = ["install", name, chart]
    if namespace:
        args += ["--namespace", namespace]
    if atomic:
        args.append("--atomic")
    if wait:
        args.append("--wait")
        args += ["--timeout", timeout]
    args += _kube_args()
    if values:
        import tempfile, yaml
        with tempfile.NamedTemporaryFile(
            suffix=".yaml", mode="w", delete=False
        ) as f:
            yaml.safe_dump(values, f)
            args += ["--values", f.name]
    if set_values:
        for k, v in set_values.items():
            args += ["--set", f"{k}={v}"]
    return _run(args)


def upgrade_release(
    name: str,
    chart: str,
    namespace: Optional[str] = None,
    values: Optional[dict[str, Any]] = None,
    set_values: Optional[dict[str, str]] = None,
    install: bool = True,
    atomic: bool = False,
    wait: bool = False,
    timeout: str = "5m",
) -> subprocess.CompletedProcess:
    """
    Upgrade an existing Helm release.
    """
    args = ["upgrade", name, chart]
    if namespace:
        args += ["--namespace", namespace]
    if install:
        args.append("--install")
    if atomic:
        args.append("--atomic")
    if wait:
        args.append("--wait")
        args += ["--timeout", timeout]
    args += _kube_args()
    if values:
        import tempfile, yaml
        with tempfile.NamedTemporaryFile(
            suffix=".yaml", mode="w", delete=False
        ) as f:
            yaml.safe_dump(values, f)
            args += ["--values", f.name]
    if set_values:
        for k, v in set_values.items():
            args += ["--set", f"{k}={v}"]
    return _run(args)


def rollback_release(
    name: str,
    revision: Optional[int] = None,
    namespace: Optional[str] = None,
    wait: bool = False,
    timeout: str = "5m",
) -> subprocess.CompletedProcess:
    """
    Roll back a release to a previous or specific revision.

    Args:
        name: Release name.
        revision: Specific revision number (defaults to previous).
        namespace: Kubernetes namespace.
        wait: Wait for resources to be ready.
        timeout: Wait timeout.
    """
    args = ["rollback", name]
    if revision:
        args.append(str(revision))
    if namespace:
        args += ["--namespace", namespace]
    if wait:
        args.append("--wait")
        args += ["--timeout", timeout]
    args += _kube_args()
    return _run(args)


def uninstall_release(
    name: str,
    namespace: Optional[str] = None,
    keep_history: bool = False,
) -> subprocess.CompletedProcess:
    """Uninstall a Helm release."""
    args = ["uninstall", name]
    if namespace:
        args += ["--namespace", namespace]
    if keep_history:
        args.append("--keep-history")
    args += _kube_args()
    return _run(args)


class HelmError(Exception):
    """Raised when a Helm command fails."""
    pass
