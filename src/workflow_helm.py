"""
Helm chart management module.

Provides HelmManager class for managing Helm charts, repositories, releases,
values, templates, dependencies, hooks, plugins, OCI registries, and history analysis.
"""

import subprocess
import json
import yaml
import os
import re
from typing import Dict, List, Optional, Any, Union
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class HookType(Enum):
    """Helm hook types."""
    PRE_INSTALL = "pre-install"
    POST_INSTALL = "post-install"
    PRE_UPGRADE = "pre-upgrade"
    POST_UPGRADE = "post-upgrade"
    PRE_DELETE = "pre-delete"
    POST_DELETE = "post-delete"
    PRE_ROLLBACK = "pre-rollback"
    POST_ROLLBACK = "post-rollback"
    TEST = "test"


class HookDeletionPolicy(Enum):
    """Helm hook deletion policies."""
    DELETE = "delete"
    RETAIN = "retain"
    DELETE_WAIT = "hook-delete-policy"
    HOOK_SUCCEEDED = "hook-succeeded"
    HOOK_FAILED = "hook-failed"


@dataclass
class ChartInfo:
    """Helm chart information."""
    name: str
    version: str
    api_version: str
    app_version: Optional[str] = None
    description: Optional[str] = None


@dataclass
class ReleaseInfo:
    """Helm release information."""
    name: str
    namespace: str
    revision: int
    status: str
    chart: str
    chart_version: str
    app_version: Optional[str] = None
    last_deployed: Optional[datetime] = None


@dataclass
class RepositoryInfo:
    """Helm repository information."""
    name: str
    url: str
    cached: bool = False


@dataclass
class HistoryEntry:
    """Helm release history entry."""
    revision: int
    app_version: Optional[str]
    chart_version: str
    status: str
    description: str
    deployed_at: Optional[datetime] = None


@dataclass
class HookConfig:
    """Helm hook configuration."""
    name: str
    hook_type: HookType
    path: str
    weight: int = 0
    deletion_policy: HookDeletionPolicy = HookDeletionPolicy.DELETE


class HelmManager:
    """
    Manager for Helm chart operations.
    
    Features:
    - Chart management: Create/manage Helm charts
    - Repository management: Add/update/remove chart repositories
    - Release management: Install/upgrade/rollback releases
    - Value management: Manage values files and overrides
    - Template rendering: Render templates locally
    - Dependency management: Manage chart dependencies
    - Hook management: Manage Helm hooks
    - Plugin management: Install/manage Helm plugins
    - OCI support: Support OCI registries for charts
    - History analysis: Analyze release history
    """
    
    def __init__(self, kube_context: Optional[str] = None, helm_path: str = "helm"):
        """
        Initialize HelmManager.
        
        Args:
            kube_context: Kubernetes context to use
            helm_path: Path to helm binary (default: 'helm')
        """
        self.helm_path = helm_path
        self.kube_context = kube_context
        self._cached_repos: Dict[str, RepositoryInfo] = {}
    
    def _run_helm(self, args: List[str], capture_output: bool = True, 
                  input_data: Optional[str] = None) -> subprocess.CompletedProcess:
        """
        Run helm command with arguments.
        
        Args:
            args: Command arguments
            capture_output: Whether to capture output
            input_data: Input data for stdin
            
        Returns:
            CompletedProcess instance
        """
        cmd = [self.helm_path] + args
        if self.kube_context:
            cmd.extend(["--kube-context", self.kube_context])
        
        return subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            input=input_data,
            check=False
        )
    
    # ==================== Chart Management ====================
    
    def create_chart(self, name: str, directory: Optional[str] = None,
                     starter: Optional[str] = None, api_version: str = "v2") -> bool:
        """
        Create a new Helm chart.
        
        Args:
            name: Chart name
            directory: Directory to create chart in
            starter: Starter chart to use
            api_version: API version (v2 for Helm 3)
            
        Returns:
            True if successful
        """
        args = ["create", name]
        if directory:
            args.extend(["--starter", starter]) if starter else None
        else:
            args.append(directory)
        
        result = self._run_helm(args)
        return result.returncode == 0
    
    def package_chart(self, chart_path: str, destination: Optional[str] = None,
                      sign: bool = False, key: Optional[str] = None,
                      keyring: Optional[str] = None, version: Optional[str] = None) -> bool:
        """
        Package a chart into a chart archive.
        
        Args:
            chart_path: Path to chart directory
            destination: Output directory
            sign: Whether to sign the package
            key: Key name for signing
            keyring: Path to keyring
            version: Override version
            
        Returns:
            True if successful
        """
        args = ["package", chart_path]
        if destination:
            args.extend(["--destination", destination])
        if sign:
            args.append("--sign")
            if key:
                args.extend(["--key", key])
            if keyring:
                args.extend(["--keyring", keyring])
        if version:
            args.extend(["--version", version])
        
        return self._run_helm(args).returncode == 0
    
    def lint_chart(self, chart_path: str, strict: bool = False,
                   values: Optional[str] = None) -> Dict[str, Any]:
        """
        Lint a Helm chart.
        
        Args:
            chart_path: Path to chart directory
            strict: Enable strict validation
            values: Values file to use
            
        Returns:
            Lint results dictionary
        """
        args = ["lint", chart_path]
        if strict:
            args.append("--strict")
        if values:
            args.extend(["--values", values])
        
        result = self._run_helm(args)
        return {
            "passed": result.returncode == 0,
            "output": result.stdout,
            "errors": result.stderr
        }
    
    def show_chart_info(self, chart_ref: str) -> Optional[ChartInfo]:
        """
        Get chart information.
        
        Args:
            chart_ref: Chart reference (name, path, or repo/chart)
            
        Returns:
            ChartInfo object or None
        """
        result = self._run_helm(["show", "all", chart_ref])
        if result.returncode != 0:
            return None
        
        try:
            data = yaml.safe_load(result.stdout)
            return ChartInfo(
                name=data.get("name", ""),
                version=data.get("version", ""),
                api_version=data.get("apiVersion", ""),
                app_version=data.get("appVersion"),
                description=data.get("description")
            )
        except yaml.YAMLError:
            return None
    
    def pull_chart(self, chart_ref: str, destination: str,
                   untar: bool = True, untardir: Optional[str] = None,
                   version: Optional[str] = None) -> bool:
        """
        Download a chart from a repository.
        
        Args:
            chart_ref: Chart reference
            destination: Download destination
            untar: Whether to extract the archive
            untardir: Directory to extract to
            version: Specific version to download
            
        Returns:
            True if successful
        """
        args = ["pull", chart_ref, "--destination", destination]
        if untar:
            args.append("--untar")
        if untardir:
            args.extend(["--untardir", untardir])
        if version:
            args.extend(["--version", version])
        
        return self._run_helm(args).returncode == 0
    
    # ==================== Repository Management ====================
    
    def add_repository(self, name: str, url: str, force_update: bool = False,
                       username: Optional[str] = None, password: Optional[str] = None,
                       ca_file: Optional[str] = None, cert_file: Optional[str] = None,
                       key_file: Optional[str] = None) -> bool:
        """
        Add a Helm chart repository.
        
        Args:
            name: Repository name
            url: Repository URL
            force_update: Force update of existing repo
            username: Username for auth
            password: Password for auth
            ca_file: CA certificate file
            cert_file: Client certificate file
            key_file: Client key file
            
        Returns:
            True if successful
        """
        args = ["repo", "add", name, url]
        if force_update:
            args.append("--force-update")
        if username:
            args.extend(["--username", username])
        if password:
            args.extend(["--password", password])
        if ca_file:
            args.extend(["--ca-file", ca_file])
        if cert_file:
            args.extend(["--cert-file", cert_file])
        if key_file:
            args.extend(["--key-file", key_file])
        
        result = self._run_helm(args)
        if result.returncode == 0:
            self._cached_repos[name] = RepositoryInfo(name=name, url=url)
        return result.returncode == 0
    
    def update_repositories(self) -> bool:
        """
        Update all Helm chart repositories.
        
        Returns:
            True if successful
        """
        result = self._run_helm(["repo", "update"])
        return result.returncode == 0
    
    def remove_repository(self, name: str) -> bool:
        """
        Remove a Helm chart repository.
        
        Args:
            name: Repository name
            
        Returns:
            True if successful
        """
        result = self._run_helm(["repo", "remove", name])
        if result.returncode == 0 and name in self._cached_repos:
            del self._cached_repos[name]
        return result.returncode == 0
    
    def list_repositories(self) -> List[RepositoryInfo]:
        """
        List all configured Helm repositories.
        
        Returns:
            List of RepositoryInfo objects
        """
        result = self._run_helm(["repo", "list", "-o", "json"])
        if result.returncode != 0:
            return []
        
        try:
            repos = json.loads(result.stdout)
            return [
                RepositoryInfo(
                    name=r.get("name", ""),
                    url=r.get("url", ""),
                    cached=r.get("cached", False)
                )
                for r in repos
            ]
        except json.JSONDecodeError:
            return []
    
    def search_repositories(self, query: str, versions: bool = True,
                           max_col_width: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Search for charts in repositories.
        
        Args:
            query: Search query
            versions: Show all versions
            max_col_width: Max column width
            
        Returns:
            List of search results
        """
        args = ["search", "repo", query]
        if versions:
            args.append("--versions")
        if max_col_width:
            args.extend(["--max-col-width", str(max_col_width)])
        
        result = self._run_helm(args)
        if result.returncode != 0:
            return []
        
        return self._parse_search_output(result.stdout)
    
    def _parse_search_output(self, output: str) -> List[Dict[str, Any]]:
        """Parse helm search repo output."""
        results = []
        lines = output.strip().split("\n")
        if len(lines) < 2:
            return results
        
        # Skip header line
        for line in lines[1:]:
            parts = line.split()
            if len(parts) >= 3:
                results.append({
                    "name": parts[0],
                    "version": parts[1],
                    "chart_version": parts[2] if len(parts) > 2 else "",
                    "app_version": parts[3] if len(parts) > 3 else "",
                    "description": " ".join(parts[4:]) if len(parts) > 4 else ""
                })
        return results
    
    # ==================== Release Management ====================
    
    def install_release(self, name: str, chart: str, namespace: Optional[str] = None,
                       values: Optional[Dict[str, Any]] = None,
                       values_files: Optional[List[str]] = None,
                       set_values: Optional[List[str]] = None,
                       wait: bool = False, wait_timeout: Optional[str] = None,
                       atomic: bool = False, dry_run: bool = False,
                       create_namespace: bool = True) -> Dict[str, Any]:
        """
        Install a Helm release.
        
        Args:
            name: Release name
            chart: Chart reference
            namespace: Namespace to install to
            values: Values dictionary
            values_files: List of values files
            set_values: List of --set values
            wait: Wait until resources are ready
            wait_timeout: Timeout for wait
            atomic: Rollback on failure
            dry_run: Simulate install
            create_namespace: Create namespace if not exists
            
        Returns:
            Result dictionary with status, manifest, notes
        """
        args = ["install", name, chart]
        
        if namespace:
            args.extend(["--namespace", namespace])
        if create_namespace:
            args.append("--create-namespace")
        if wait:
            args.append("--wait")
        if wait_timeout:
            args.extend(["--timeout", wait_timeout])
        if atomic:
            args.append("--atomic")
        if dry_run:
            args.append("--dry-run")
        
        if values_files:
            for vf in values_files:
                args.extend(["--values", vf])
        
        if set_values:
            for sv in set_values:
                args.extend(["--set", sv])
        
        if values:
            values_yaml = yaml.dump(values)
            result = self._run_helm(args, input_data=values_yaml)
        else:
            result = self._run_helm(args)
        
        return self._parse_release_result(result, "installed")
    
    def upgrade_release(self, name: str, chart: str, namespace: Optional[str] = None,
                        values: Optional[Dict[str, Any]] = None,
                        values_files: Optional[List[str]] = None,
                        set_values: Optional[List[str]] = None,
                        wait: bool = False, wait_timeout: Optional[str] = None,
                        atomic: bool = False, dry_run: bool = False,
                        install: bool = True, reset_values: bool = False,
                        reuse_values: bool = False) -> Dict[str, Any]:
        """
        Upgrade a Helm release.
        
        Args:
            name: Release name
            chart: Chart reference
            namespace: Namespace
            values: Values dictionary
            values_files: List of values files
            set_values: List of --set values
            wait: Wait until resources are ready
            wait_timeout: Timeout for wait
            atomic: Rollback on failure
            dry_run: Simulate upgrade
            install: Create release if not exists
            reset_values: Reset values to chart defaults
            reuse_values: Reuse previous values
            
        Returns:
            Result dictionary
        """
        args = ["upgrade", name, chart]
        
        if namespace:
            args.extend(["--namespace", namespace])
        if wait:
            args.append("--wait")
        if wait_timeout:
            args.extend(["--timeout", wait_timeout])
        if atomic:
            args.append("--atomic")
        if dry_run:
            args.append("--dry-run")
        if install:
            args.append("--install")
        if reset_values:
            args.append("--reset-values")
        if reuse_values:
            args.append("--reuse-values")
        
        if values_files:
            for vf in values_files:
                args.extend(["--values", vf])
        
        if set_values:
            for sv in set_values:
                args.extend(["--set", sv])
        
        if values:
            values_yaml = yaml.dump(values)
            result = self._run_helm(args, input_data=values_yaml)
        else:
            result = self._run_helm(args)
        
        return self._parse_release_result(result, "upgraded")
    
    def rollback_release(self, name: str, revision: Optional[int] = None,
                         namespace: Optional[str] = None, wait: bool = False,
                         wait_timeout: Optional[str] = None,
                         dry_run: bool = False) -> Dict[str, Any]:
        """
        Rollback a Helm release to a previous revision.
        
        Args:
            name: Release name
            revision: Revision to rollback to (default: previous)
            namespace: Namespace
            wait: Wait until resources are ready
            wait_timeout: Timeout for wait
            dry_run: Simulate rollback
            
        Returns:
            Result dictionary
        """
        args = ["rollback", name]
        
        if revision:
            args.append(str(revision))
        if namespace:
            args.extend(["--namespace", namespace])
        if wait:
            args.append("--wait")
        if wait_timeout:
            args.extend(["--timeout", wait_timeout])
        if dry_run:
            args.append("--dry-run")
        
        result = self._run_helm(args)
        return self._parse_release_result(result, "rolled back")
    
    def uninstall_release(self, name: str, namespace: Optional[str] = None,
                          keep_history: bool = False,
                          cascade: bool = True) -> Dict[str, Any]:
        """
        Uninstall a Helm release.
        
        Args:
            name: Release name
            namespace: Namespace
            keep_history: Keep release history
            cascade: Cascade delete resources
            
        Returns:
            Result dictionary
        """
        args = ["uninstall", name]
        
        if namespace:
            args.extend(["--namespace", namespace])
        if keep_history:
            args.append("--keep-history")
        if not cascade:
            args.append("--no-cascade")
        
        result = self._run_helm(args)
        return {
            "success": result.returncode == 0,
            "output": result.stdout,
            "error": result.stderr if result.returncode != 0 else None
        }
    
    def list_releases(self, namespace: Optional[str] = None,
                      all_namespaces: bool = False,
                      status_filter: Optional[str] = None) -> List[ReleaseInfo]:
        """
        List Helm releases.
        
        Args:
            namespace: Filter by namespace
            all_namespaces: List across all namespaces
            status_filter: Filter by status (deployed, failed, etc.)
            
        Returns:
            List of ReleaseInfo objects
        """
        args = ["list", "-o", "json"]
        
        if namespace:
            args.extend(["--namespace", namespace])
        if all_namespaces:
            args.append("--all-namespaces")
        if status_filter:
            args.extend(["--status", status_filter])
        
        result = self._run_helm(args)
        if result.returncode != 0:
            return []
        
        try:
            releases = json.loads(result.stdout)
            return [
                ReleaseInfo(
                    name=r.get("name", ""),
                    namespace=r.get("namespace", ""),
                    revision=int(r.get("revision", 0)),
                    status=r.get("status", ""),
                    chart=r.get("chart", ""),
                    chart_version=r.get("chart_version", ""),
                    app_version=r.get("app_version"),
                    last_deployed=self._parse_datetime(r.get("last_deployed"))
                )
                for r in releases
            ]
        except (json.JSONDecodeError, ValueError):
            return []
    
    def get_release_status(self, name: str, namespace: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get detailed status of a release.
        
        Args:
            name: Release name
            namespace: Namespace
            
        Returns:
            Status dictionary or None
        """
        args = ["status", name, "-o", "json"]
        if namespace:
            args.extend(["--namespace", namespace])
        
        result = self._run_helm(args)
        if result.returncode != 0:
            return None
        
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return None
    
    def get_release_values(self, name: str, namespace: Optional[str] = None,
                           all_values: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get values for a release.
        
        Args:
            name: Release name
            namespace: Namespace
            all_values: Get all values including defaults
            
        Returns:
            Values dictionary or None
        """
        args = ["get", "values", name, "-o", "yaml"]
        if namespace:
            args.extend(["--namespace", namespace])
        if not all_values:
            args.append("--all")
        
        result = self._run_helm(args)
        if result.returncode != 0:
            return None
        
        try:
            return yaml.safe_load(result.stdout) or {}
        except yaml.YAMLError:
            return None
    
    def get_release_manifest(self, name: str, namespace: Optional[str] = None,
                             revision: Optional[int] = None) -> Optional[str]:
        """
        Get manifest for a release.
        
        Args:
            name: Release name
            namespace: Namespace
            revision: Specific revision
            
        Returns:
            Manifest YAML string or None
        """
        args = ["get", "manifest", name]
        if namespace:
            args.extend(["--namespace", namespace])
        if revision:
            args.extend(["--revision", str(revision)])
        
        result = self._run_helm(args)
        if result.returncode != 0:
            return None
        return result.stdout
    
    def _parse_release_result(self, result: subprocess.CompletedProcess,
                              action: str) -> Dict[str, Any]:
        """Parse helm install/upgrade/rollback result."""
        if result.returncode == 0:
            # Try to extract manifest from --dry-run output
            manifest = ""
            notes = ""
            
            if "MANIFEST:" in result.stdout:
                parts = result.stdout.split("MANIFEST:")
                manifest = parts[1].strip() if len(parts) > 1 else ""
            
            return {
                "success": True,
                "action": action,
                "output": result.stdout,
                "manifest": manifest,
                "notes": notes
            }
        else:
            return {
                "success": False,
                "action": action,
                "error": result.stderr,
                "output": result.stdout
            }
    
    # ==================== Value Management ====================
    
    def merge_values(self, *values_list: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge multiple values dictionaries.
        
        Args:
            *values_list: Values dictionaries to merge
            
        Returns:
            Merged values dictionary
        """
        result = {}
        for values in values_list:
            if values:
                result = self._deep_merge(result, values)
        return result
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def load_values_file(self, path: str) -> Optional[Dict[str, Any]]:
        """
        Load values from a YAML file.
        
        Args:
            path: Path to values file
            
        Returns:
            Values dictionary or None
        """
        try:
            with open(path, 'r') as f:
                return yaml.safe_load(f) or {}
        except (IOError, yaml.YAMLError):
            return None
    
    def save_values_file(self, values: Dict[str, Any], path: str) -> bool:
        """
        Save values to a YAML file.
        
        Args:
            values: Values dictionary
            path: Output path
            
        Returns:
            True if successful
        """
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w') as f:
                yaml.dump(values, f, default_flow_style=False)
            return True
        except IOError:
            return False
    
    def extract_values(self, chart: str, values_file: Optional[str] = None,
                       all_values: bool = False) -> Optional[Dict[str, Any]]:
        """
        Extract default values from a chart.
        
        Args:
            chart: Chart reference
            values_file: Specific values file
            all_values: Get all values
            
        Returns:
            Values dictionary or None
        """
        args = ["show", "values", chart]
        if values_file:
            args.extend(["--version", values_file])
        
        result = self._run_helm(args)
        if result.returncode != 0:
            return None
        
        try:
            return yaml.safe_load(result.stdout) or {}
        except yaml.YAMLError:
            return None
    
    # ==================== Template Rendering ====================
    
    def render_templates(self, chart: str, values: Optional[Dict[str, Any]] = None,
                         values_files: Optional[List[str]] = None,
                         set_values: Optional[List[str]] = None,
                         namespace: Optional[str] = None,
                         name: Optional[str] = None) -> Dict[str, Any]:
        """
        Render chart templates locally.
        
        Args:
            chart: Chart reference
            values: Values dictionary
            values_files: Values files
            set_values: Set values
            namespace: Namespace
            name: Release name (for validation)
            
        Returns:
            Dictionary with rendered templates
        """
        args = ["template", chart]
        
        if name:
            args.append(name)
        if namespace:
            args.extend(["--namespace", namespace])
        
        if values_files:
            for vf in values_files:
                args.extend(["--values", vf])
        
        if set_values:
            for sv in set_values:
                args.extend(["--set", sv])
        
        if values:
            values_yaml = yaml.dump(values)
            result = self._run_helm(args, input_data=values_yaml)
        else:
            result = self._run_helm(args)
        
        if result.returncode == 0:
            return {
                "success": True,
                "manifest": result.stdout,
                "templates": self._parse_manifest(result.stdout)
            }
        else:
            return {
                "success": False,
                "error": result.stderr
            }
    
    def render_templates_with_debug(self, chart: str, values: Optional[Dict[str, Any]] = None,
                                     values_files: Optional[List[str]] = None,
                                     namespace: Optional[str] = None,
                                     name: Optional[str] = None) -> Dict[str, Any]:
        """
        Render templates with debug output.
        
        Args:
            chart: Chart reference
            values: Values dictionary
            values_files: Values files
            namespace: Namespace
            name: Release name
            
        Returns:
            Dictionary with rendered templates and debug info
        """
        args = ["template", chart]
        
        if name:
            args.append(name)
        if namespace:
            args.extend(["--namespace", namespace])
        args.append("--debug")
        
        if values_files:
            for vf in values_files:
                args.extend(["--values", vf])
        
        if values:
            values_yaml = yaml.dump(values)
            result = self._run_helm(args, input_data=values_yaml)
        else:
            result = self._run_helm(args)
        
        return {
            "success": result.returncode == 0,
            "manifest": result.stdout,
            "stderr": result.stderr,
            "templates": self._parse_manifest(result.stdout) if result.returncode == 0 else []
        }
    
    def _parse_manifest(self, manifest: str) -> List[Dict[str, Any]]:
        """Parse YAML manifest into resources."""
        resources = []
        documents = manifest.strip().split("---")
        
        for doc in documents:
            doc = doc.strip()
            if not doc:
                continue
            try:
                resource = yaml.safe_load(doc)
                if resource:
                    resources.append({
                        "kind": resource.get("kind", ""),
                        "name": resource.get("metadata", {}).get("name", ""),
                        "namespace": resource.get("metadata", {}).get("namespace", ""),
                        "api_version": resource.get("apiVersion", ""),
                        "yaml": doc
                    })
            except yaml.YAMLError:
                continue
        
        return resources
    
    # ==================== Dependency Management ====================
    
    def update_dependencies(self, chart_path: str, keyring: Optional[str] = None) -> bool:
        """
        Update chart dependencies.
        
        Args:
            chart_path: Path to chart directory
            keyring: Keyring for verification
            
        Returns:
            True if successful
        """
        args = ["dependency", "update", chart_path]
        if keyring:
            args.extend(["--keyring", keyring])
        
        return self._run_helm(args).returncode == 0
    
    def build_dependencies(self, chart_path: str, verify: bool = False,
                           keyring: Optional[str] = None) -> bool:
        """
        Build chart dependencies.
        
        Args:
            chart_path: Path to chart directory
            verify: Verify packages
            keyring: Keyring for verification
            
        Returns:
            True if successful
        """
        args = ["dependency", "build", chart_path]
        if verify:
            args.append("--verify")
        if keyring:
            args.extend(["--keyring", keyring])
        
        return self._run_helm(args).returncode == 0
    
    def list_dependencies(self, chart_path: str) -> List[Dict[str, Any]]:
        """
        List chart dependencies.
        
        Args:
            chart_path: Path to chart directory
            
        Returns:
            List of dependencies
        """
        result = self._run_helm(["dependency", "list", chart_path])
        if result.returncode != 0:
            return []
        
        deps = []
        lines = result.stdout.strip().split("\n")
        if len(lines) < 2:
            return deps
        
        # Skip header line
        for line in lines[1:]:
            parts = line.split()
            if len(parts) >= 3:
                deps.append({
                    "name": parts[0],
                    "version": parts[1],
                    "repository": parts[2] if len(parts) > 2 else "",
                    "status": parts[3] if len(parts) > 3 else ""
                })
        return deps
    
    # ==================== Hook Management ====================
    
    def create_hook(self, hook_config: HookConfig) -> str:
        """
        Create a hook annotation YAML snippet.
        
        Args:
            hook_config: Hook configuration
            
        Returns:
            YAML annotation string
        """
        hook_yaml = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": hook_config.name,
                "annotations": {
                    "helm.sh/hook": hook_config.hook_type.value,
                    "helm.sh/hook-weight": str(hook_config.weight),
                    "helm.sh/hook-delete-policy": hook_config.deletion_policy.value
                }
            },
            "data": {}
        }
        return yaml.dump(hook_yaml, default_flow_style=False)
    
    def add_hook_annotation(self, resource: Dict[str, Any], hook_type: HookType,
                            weight: int = 0,
                            deletion_policy: HookDeletionPolicy = HookDeletionPolicy.DELETE) -> Dict[str, Any]:
        """
        Add hook annotations to a resource.
        
        Args:
            resource: Resource dictionary
            hook_type: Type of hook
            weight: Hook weight (execution order)
            deletion_policy: Deletion policy
            
        Returns:
            Modified resource
        """
        if "metadata" not in resource:
            resource["metadata"] = {}
        if "annotations" not in resource["metadata"]:
            resource["metadata"]["annotations"] = {}
        
        resource["metadata"]["annotations"]["helm.sh/hook"] = hook_type.value
        resource["metadata"]["annotations"]["helm.sh/hook-weight"] = str(weight)
        resource["metadata"]["annotations"]["helm.sh/hook-delete-policy"] = deletion_policy.value
        
        return resource
    
    def remove_hook_annotation(self, resource: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove hook annotations from a resource.
        
        Args:
            resource: Resource dictionary
            
        Returns:
            Modified resource
        """
        if "metadata" in resource and "annotations" in resource["metadata"]:
            for key in ["helm.sh/hook", "helm.sh/hook-weight", "helm.sh/hook-delete-policy"]:
                resource["metadata"]["annotations"].pop(key, None)
        return resource
    
    def get_hooks_from_manifest(self, manifest: str) -> List[Dict[str, Any]]:
        """
        Extract hooks from a rendered manifest.
        
        Args:
            manifest: YAML manifest
            
        Returns:
            List of hook resources
        """
        hooks = []
        documents = manifest.strip().split("---")
        
        for doc in documents:
            doc = doc.strip()
            if not doc:
                continue
            try:
                resource = yaml.safe_load(doc)
                if resource and "metadata" in resource and "annotations" in resource["metadata"]:
                    hook_annotation = resource["metadata"]["annotations"].get("helm.sh/hook", "")
                    if hook_annotation:
                        hooks.append({
                            "name": resource["metadata"].get("name", ""),
                            "kind": resource.get("kind", ""),
                            "hook_types": hook_annotation.split(","),
                            "weight": resource["metadata"]["annotations"].get("helm.sh/hook-weight", "0"),
                            "deletion_policy": resource["metadata"]["annotations"].get("helm.sh/hook-delete-policy", ""),
                            "resource": resource
                        })
            except yaml.YAMLError:
                continue
        
        return hooks
    
    def test_hooks(self, name: str, namespace: Optional[str] = None,
                   cleanup: bool = True, parallel: bool = False) -> Dict[str, Any]:
        """
        Run hooks/tests for a release.
        
        Args:
            name: Release name
            namespace: Namespace
            cleanup: Cleanup test pods after test
            parallel: Run tests in parallel
            
        Returns:
            Test results
        """
        args = ["test", name]
        
        if namespace:
            args.extend(["--namespace", namespace])
        if cleanup:
            args.append("--cleanup")
        if parallel:
            args.append("--parallel")
        
        result = self._run_helm(args)
        return {
            "success": result.returncode == 0,
            "output": result.stdout,
            "error": result.stderr
        }
    
    # ==================== Plugin Management ====================
    
    def list_plugins(self) -> List[Dict[str, Any]]:
        """
        List installed Helm plugins.
        
        Returns:
            List of plugins
        """
        result = self._run_helm(["plugin", "list", "-o", "json"])
        if result.returncode != 0:
            return []
        
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return []
    
    def install_plugin(self, path: str, version: Optional[str] = None) -> bool:
        """
        Install a Helm plugin.
        
        Args:
            path: Path or URL to plugin
            version: Plugin version
            
        Returns:
            True if successful
        """
        args = ["plugin", "install", path]
        if version:
            args.extend(["--version", version])
        
        return self._run_helm(args).returncode == 0
    
    def uninstall_plugin(self, name: str) -> bool:
        """
        Uninstall a Helm plugin.
        
        Args:
            name: Plugin name
            
        Returns:
            True if successful
        """
        return self._run_helm(["plugin", "uninstall", name]).returncode == 0
    
    def update_plugin(self, name: str) -> bool:
        """
        Update a Helm plugin.
        
        Args:
            name: Plugin name
            
        Returns:
            True if successful
        """
        return self._run_helm(["plugin", "update", name]).returncode == 0
    
    # ==================== OCI Support ====================
    
    def login_to_oci(self, registry: str, username: Optional[str] = None,
                     password: Optional[str] = None,
                     password_stdin: bool = False) -> bool:
        """
        Login to an OCI registry.
        
        Args:
            registry: Registry URL
            username: Username
            password: Password
            password_stdin: Read password from stdin
            
        Returns:
            True if successful
        """
        args = ["registry", "login", registry]
        
        if username:
            args.extend(["--username", username])
        if password_stdin:
            args.append("--password-stdin")
        elif password:
            # Use --password flag if not using stdin
            args.extend(["--password", password])
        
        if password_stdin and password:
            result = self._run_helm(args, input_data=password)
        else:
            result = self._run_helm(args)
        
        return result.returncode == 0
    
    def logout_from_oci(self, registry: str) -> bool:
        """
        Logout from an OCI registry.
        
        Args:
            registry: Registry URL
            
        Returns:
            True if successful
        """
        return self._run_helm(["registry", "logout", registry]).returncode == 0
    
    def pull_from_oci(self, chart_ref: str, destination: str,
                      username: Optional[str] = None,
                      password: Optional[str] = None,
                      version: Optional[str] = None) -> bool:
        """
        Pull a chart from an OCI registry.
        
        Args:
            chart_ref: Chart reference (e.g., oci://registry/chart)
            destination: Destination directory
            username: Username for auth
            password: Password for auth
            version: Chart version
            
        Returns:
            True if successful
        """
        args = ["pull", chart_ref, "--destination", destination]
        
        if username:
            args.extend(["--username", username])
        if password:
            args.extend(["--password", password])
        if version:
            args.extend(["--version", version])
        
        return self._run_helm(args).returncode == 0
    
    def install_from_oci(self, name: str, chart_ref: str,
                         username: Optional[str] = None,
                         password: Optional[str] = None,
                         namespace: Optional[str] = None,
                         values: Optional[Dict[str, Any]] = None,
                         version: Optional[str] = None,
                         **kwargs) -> Dict[str, Any]:
        """
        Install a chart from an OCI registry.
        
        Args:
            name: Release name
            chart_ref: Chart reference (oci://registry/chart)
            username: Username for auth
            password: Password for auth
            namespace: Namespace
            values: Values dictionary
            version: Chart version
            **kwargs: Additional arguments for install
            
        Returns:
            Result dictionary
        """
        args = ["install", name, chart_ref]
        
        if namespace:
            args.extend(["--namespace", namespace])
        if username:
            args.extend(["--username", username])
        if password:
            args.extend(["--password", password])
        if version:
            args.extend(["--version", version])
        
        for key, value in kwargs.items():
            if value is not None:
                args.extend([f"--{key.replace('_', '-')}", str(value)])
        
        if values:
            values_yaml = yaml.dump(values)
            result = self._run_helm(args, input_data=values_yaml)
        else:
            result = self._run_helm(args)
        
        return self._parse_release_result(result, "installed")
    
    # ==================== History Analysis ====================
    
    def get_release_history(self, name: str, namespace: Optional[str] = None,
                             max_revisions: Optional[int] = None) -> List[HistoryEntry]:
        """
        Get release history.
        
        Args:
            name: Release name
            namespace: Namespace
            max_revisions: Limit number of revisions
            
        Returns:
            List of HistoryEntry objects
        """
        args = ["history", name, "-o", "json"]
        if namespace:
            args.extend(["--namespace", namespace])
        if max_revisions:
            args.extend(["--max", str(max_revisions)])
        
        result = self._run_helm(args)
        if result.returncode != 0:
            return []
        
        try:
            history = json.loads(result.stdout)
            return [
                HistoryEntry(
                    revision=int(h.get("revision", 0)),
                    app_version=h.get("app_version"),
                    chart_version=h.get("chart_version", ""),
                    status=h.get("status", ""),
                    description=h.get("description", ""),
                    deployed_at=self._parse_datetime(h.get("deployed_at"))
                )
                for h in history
            ]
        except (json.JSONDecodeError, ValueError):
            return []
    
    def analyze_history_trends(self, name: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze release history for trends.
        
        Args:
            name: Release name
            namespace: Namespace
            
        Returns:
            Analysis dictionary with trends and statistics
        """
        history = self.get_release_history(name, namespace)
        if not history:
            return {"error": "No history found"}
        
        statuses = [h.status for h in history]
        revisions = [h.revision for h in history]
        
        # Count status occurrences
        status_counts = {}
        for status in statuses:
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Calculate revision statistics
        revisions.sort()
        mid = len(revisions) // 2
        median_revision = revisions[mid] if revisions else 0
        
        # Find rollback patterns
        rollback_count = 0
        for i, entry in enumerate(history):
            if "rollback" in entry.description.lower() or "rolled back" in entry.description.lower():
                rollback_count += 1
        
        # Current and previous status
        current_status = statuses[0] if statuses else "unknown"
        previous_status = statuses[1] if len(statuses) > 1 else "none"
        
        # Time analysis
        deployed_times = [h.deployed_at for h in history if h.deployed_at]
        deployed_times.sort()
        
        avg_deploy_interval = None
        if len(deployed_times) >= 2:
            intervals = []
            for i in range(1, len(deployed_times)):
                delta = (deployed_times[i-1] - deployed_times[i]).total_seconds()
                intervals.append(abs(delta))
            avg_deploy_interval = sum(intervals) / len(intervals) if intervals else None
        
        return {
            "release_name": name,
            "total_revisions": len(history),
            "current_revision": revisions[0] if revisions else 0,
            "median_revision": median_revision,
            "current_status": current_status,
            "previous_status": previous_status,
            "status_distribution": status_counts,
            "rollback_count": rollback_count,
            "first_deployed": deployed_times[-1] if deployed_times else None,
            "last_deployed": deployed_times[0] if deployed_times else None,
            "avg_deploy_interval_seconds": avg_deploy_interval,
            "is_stable": current_status == "deployed" and status_counts.get("failed", 0) == 0,
            "has_rollbacks": rollback_count > 0
        }
    
    def find_problematic_revisions(self, name: str, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Find revisions with issues.
        
        Args:
            name: Release name
            namespace: Namespace
            
        Returns:
            List of problematic revisions
        """
        history = self.get_release_history(name, namespace)
        issues = []
        
        for entry in history:
            if entry.status in ["failed", "pending", "pending-install", "pending-upgrade"]:
                issues.append({
                    "revision": entry.revision,
                    "status": entry.status,
                    "description": entry.description,
                    "deployed_at": entry.deployed_at
                })
            elif "error" in entry.description.lower() or "fail" in entry.description.lower():
                issues.append({
                    "revision": entry.revision,
                    "status": entry.status,
                    "description": entry.description,
                    "deployed_at": entry.deployed_at,
                    "warning": "Failure mentioned in description"
                })
        
        return issues
    
    def suggest_rollback_revision(self, name: str, namespace: Optional[str] = None) -> Optional[int]:
        """
        Suggest a revision to rollback to.
        
        Args:
            name: Release name
            namespace: Namespace
            
        Returns:
            Revision number to rollback to, or None
        """
        history = self.get_release_history(name, namespace)
        if not history:
            return None
        
        # Find the last successful deployment
        for entry in history:
            if entry.status == "deployed":
                return entry.revision
        
        # If no deployed found, return second revision (usually the previous)
        if len(history) > 1:
            return history[1].revision
        
        return None
    
    def compare_revisions(self, name: str, revision1: int, revision2: int,
                         namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Compare two revisions of a release.
        
        Args:
            name: Release name
            revision1: First revision
            revision2: Second revision
            namespace: Namespace
            
        Returns:
            Comparison dictionary
        """
        manifest1 = self.get_release_manifest(name, namespace, revision1)
        manifest2 = self.get_release_manifest(name, namespace, revision2)
        
        if not manifest1 or not manifest2:
            return {"error": "Could not retrieve manifests for one or both revisions"}
        
        resources1 = self._parse_manifest(manifest1)
        resources2 = self._parse_manifest(manifest2)
        
        # Create lookup by resource key
        def resource_key(r): return f"{r['kind']}/{r['name']}"
        
        res1_map = {resource_key(r): r for r in resources1}
        res2_map = {resource_key(r): r for r in resources2}
        
        added = [r for r in resources2 if resource_key(r) not in res1_map]
        removed = [r for r in resources1 if resource_key(r) not in res2_map]
        modified = []
        
        for key in res1_map:
            if key in res2_map:
                if res1_map[key]["yaml"] != res2_map[key]["yaml"]:
                    modified.append({
                        "resource": key,
                        "old": res1_map[key],
                        "new": res2_map[key]
                    })
        
        return {
            "revision1": revision1,
            "revision2": revision2,
            "resources_revision1": len(resources1),
            "resources_revision2": len(resources2),
            "added": added,
            "removed": removed,
            "modified": modified
        }
    
    # ==================== Utility Methods ====================
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string from Helm output."""
        if not dt_str:
            return None
        try:
            # Try ISO format first
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except ValueError:
            try:
                # Try common helm format
                return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f %Z")
            except ValueError:
                try:
                    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return None
    
    def check_helm_version(self) -> Optional[str]:
        """
        Check Helm version.
        
        Returns:
            Version string or None
        """
        result = self._run_helm(["version", "--short"])
        if result.returncode == 0:
            return result.stdout.strip()
        return None
    
    def verify_chart(self, chart_path: str, keyring: Optional[str] = None) -> Dict[str, Any]:
        """
        Verify a chart package.
        
        Args:
            chart_path: Path to chart archive
            keyring: Keyring for verification
            
        Returns:
            Verification result
        """
        args = ["verify", chart_path]
        if keyring:
            args.extend(["--keyring", keyring])
        
        result = self._run_helm(args)
        return {
            "verified": result.returncode == 0,
            "output": result.stdout,
            "error": result.stderr
        }
    
    def sign_chart(self, chart_path: str, keyring: str, key_name: str,
                   cert_file: Optional[str] = None) -> bool:
        """
        Sign a chart package.
        
        Args:
            chart_path: Path to chart archive
            keyring: Path to keyring
            key_name: Key name to use
            cert_file: Certificate file
            
        Returns:
            True if successful
        """
        args = ["sign", chart_path, "--keyring", keyring, "--key", key_name]
        if cert_file:
            args.extend(["--cert", cert_file])
        
        return self._run_helm(args).returncode == 0
