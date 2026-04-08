"""Helm action module for RabAI AutoClick.

Provides Helm package manager operations for Kubernetes
including chart management, release operations, and repository control.
"""

import os
import sys
import time
import subprocess
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class HelmRelease:
    """Represents a Helm release.
    
    Attributes:
        name: Release name.
        namespace: Release namespace.
        revision: Revision number.
        status: Release status.
        chart: Chart name.
        app_version: Application version.
    """
    name: str
    namespace: str
    revision: str
    status: str
    chart: str
    app_version: str


class HelmClient:
    """Helm client for Kubernetes package management.
    
    Provides methods for managing Helm charts,
    releases, and repositories.
    """
    
    def __init__(
        self,
        kubeconfig: Optional[str] = None,
        context: Optional[str] = None,
        helm_path: str = "helm"
    ) -> None:
        """Initialize Helm client.
        
        Args:
            kubeconfig: Path to kubeconfig file.
            context: Kubernetes context to use.
            helm_path: Path to helm binary.
        """
        self.kubeconfig = kubeconfig
        self.context = context
        self.helm_path = helm_path
    
    def _run_command(self, args: List[str], timeout: int = 300) -> subprocess.CompletedProcess:
        """Run a helm command.
        
        Args:
            args: Command arguments.
            timeout: Command timeout.
            
        Returns:
            CompletedProcess result.
        """
        cmd = [self.helm_path] + args
        
        env = os.environ.copy()
        if self.kubeconfig:
            env["KUBECONFIG"] = self.kubeconfig
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                timeout=timeout
            )
            return result
        except subprocess.TimeoutExpired:
            raise Exception(f"Helm command timed out after {timeout}s")
        except Exception as e:
            raise Exception(f"Helm command failed: {str(e)}")
    
    def connect(self) -> bool:
        """Test if Helm is available and cluster is reachable.
        
        Returns:
            True if Helm is available, False otherwise.
        """
        try:
            result = self._run_command(["version", "--short"], timeout=30)
            return result.returncode == 0
        except Exception:
            return False
    
    def list_releases(
        self,
        namespace: Optional[str] = None,
        all_namespaces: bool = False,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List Helm releases.
        
        Args:
            namespace: Specific namespace.
            all_namespaces: List across all namespaces.
            status: Filter by status (deployed, failed, pending, etc.).
            
        Returns:
            List of release information.
        """
        args = ["list", "--output", "json"]
        
        if all_namespaces:
            args.append("--all-namespaces")
        elif namespace:
            args.extend(["--namespace", namespace])
        
        if status:
            args.extend(["--filter", status])
        
        try:
            result = self._run_command(args)
            
            if result.returncode != 0:
                return []
            
            import json
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                return []
        
        except Exception:
            return []
    
    def get_release(self, name: str, namespace: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get detailed information about a release.
        
        Args:
            name: Release name.
            namespace: Release namespace.
            
        Returns:
            Release information or None.
        """
        args = ["get", "all", name]
        
        if namespace:
            args.extend(["--namespace", namespace])
        
        try:
            result = self._run_command(args)
            
            if result.returncode == 0:
                return {"name": name, "output": result.stdout}
            
            return None
        
        except Exception:
            return None
    
    def install_release(
        self,
        name: str,
        chart: str,
        namespace: Optional[str] = None,
        values: Optional[Dict[str, Any]] = None,
        version: Optional[str] = None,
        wait: bool = True,
        timeout: int = 300
    ) -> bool:
        """Install a Helm release.
        
        Args:
            name: Release name.
            chart: Chart name or path.
            namespace: Target namespace.
            values: Optional values to set.
            version: Chart version.
            wait: Wait for deployment.
            timeout: Timeout in seconds.
            
        Returns:
            True if install succeeded.
        """
        args = ["install", name, chart]
        
        if namespace:
            args.extend(["--namespace", namespace])
        
        if values:
            for key, value in values.items():
                args.extend(["--set", f"{key}={value}"])
        
        if version:
            args.extend(["--version", version])
        
        if wait:
            args.append("--wait")
            args.extend(["--timeout", f"{timeout}s"])
        
        try:
            result = self._run_command(args, timeout=timeout)
            return result.returncode == 0
        except Exception:
            return False
    
    def upgrade_release(
        self,
        name: str,
        chart: str,
        namespace: Optional[str] = None,
        values: Optional[Dict[str, Any]] = None,
        version: Optional[str] = None,
        wait: bool = True,
        timeout: int = 300
    ) -> bool:
        """Upgrade a Helm release.
        
        Args:
            name: Release name.
            chart: Chart name or path.
            namespace: Target namespace.
            values: Optional values to set.
            version: Chart version.
            wait: Wait for deployment.
            timeout: Timeout in seconds.
            
        Returns:
            True if upgrade succeeded.
        """
        args = ["upgrade", name, chart]
        
        if namespace:
            args.extend(["--namespace", namespace])
        
        if values:
            for key, value in values.items():
                args.extend(["--set", f"{key}={value}"])
        
        if version:
            args.extend(["--version", version])
        
        if wait:
            args.append("--wait")
            args.extend(["--timeout", f"{timeout}s"])
        
        try:
            result = self._run_command(args, timeout=timeout)
            return result.returncode == 0
        except Exception:
            return False
    
    def rollback_release(
        self,
        name: str,
        revision: Optional[int] = None,
        namespace: Optional[str] = None,
        wait: bool = True
    ) -> bool:
        """Rollback a Helm release.
        
        Args:
            name: Release name.
            revision: Specific revision to rollback to (default: previous).
            namespace: Release namespace.
            wait: Wait for deployment.
            
        Returns:
            True if rollback succeeded.
        """
        args = ["rollback", name]
        
        if revision:
            args.append(str(revision))
        else:
            args.append("0")
        
        if namespace:
            args.extend(["--namespace", namespace])
        
        if wait:
            args.append("--wait")
        
        try:
            result = self._run_command(args)
            return result.returncode == 0
        except Exception:
            return False
    
    def uninstall_release(
        self,
        name: str,
        namespace: Optional[str] = None,
        keep_history: bool = False
    ) -> bool:
        """Uninstall a Helm release.
        
        Args:
            name: Release name.
            namespace: Release namespace.
            keep_history: Keep release history.
            
        Returns:
            True if uninstall succeeded.
        """
        args = ["uninstall", name]
        
        if namespace:
            args.extend(["--namespace", namespace])
        
        if keep_history:
            args.append("--keep-history")
        
        try:
            result = self._run_command(args)
            return result.returncode == 0
        except Exception:
            return False
    
    def pull_chart(
        self,
        chart: str,
        destination: str = ".",
        version: Optional[str] = None,
        untar: bool = True
    ) -> bool:
        """Pull a chart from a repository.
        
        Args:
            chart: Chart name.
            destination: Download destination.
            version: Specific chart version.
            untar: Untar the chart.
            
        Returns:
            True if pull succeeded.
        """
        args = ["pull", chart, "--destination", destination]
        
        if version:
            args.extend(["--version", version])
        
        if untar:
            args.append("--untar")
        
        try:
            result = self._run_command(args)
            return result.returncode == 0
        except Exception:
            return False
    
    def search_repo(
        self,
        query: str,
        versions: bool = False
    ) -> List[Dict[str, Any]]:
        """Search for charts in repositories.
        
        Args:
            query: Search query.
            versions: Show all versions.
            
        Returns:
            List of matching charts.
        """
        args = ["search", "repo", query]
        
        if versions:
            args.append("--versions")
        
        try:
            result = self._run_command(args)
            
            if result.returncode != 0:
                return []
            
            lines = result.stdout.strip().split("\n")
            charts = []
            
            for line in lines[1:]:
                parts = line.split()
                if len(parts) >= 2:
                    charts.append({
                        "name": parts[0],
                        "version": parts[1] if len(parts) > 1 else "",
                        "description": " ".join(parts[2:]) if len(parts) > 2 else ""
                    })
            
            return charts
        
        except Exception:
            return []
    
    def add_repository(
        self,
        name: str,
        url: str
    ) -> bool:
        """Add a chart repository.
        
        Args:
            name: Repository name.
            url: Repository URL.
            
        Returns:
            True if add succeeded.
        """
        try:
            result = self._run_command(["repo", "add", name, url])
            return result.returncode == 0
        except Exception:
            return False
    
    def update_repositories(self) -> bool:
        """Update chart repositories.
        
        Returns:
            True if update succeeded.
        """
        try:
            result = self._run_command(["repo", "update"])
            return result.returncode == 0
        except Exception:
            return False
    
    def list_repositories(self) -> List[Dict[str, str]]:
        """List added chart repositories.
        
        Returns:
            List of repository information.
        """
        try:
            result = self._run_command(["repo", "list", "--output", "json"])
            
            if result.returncode != 0:
                return []
            
            import json
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                return []
        
        except Exception:
            return []
    
    def template_chart(
        self,
        name: str,
        chart: str,
        namespace: Optional[str] = None,
        values: Optional[Dict[str, Any]] = None,
        version: Optional[str] = None
    ) -> str:
        """Render chart templates locally.
        
        Args:
            name: Release name.
            chart: Chart path.
            namespace: Target namespace.
            values: Optional values.
            version: Chart version.
            
        Returns:
            Rendered templates.
        """
        args = ["template", name, chart]
        
        if namespace:
            args.extend(["--namespace", namespace])
        
        if values:
            for key, value in values.items():
                args.extend(["--set", f"{key}={value}"])
        
        if version:
            args.extend(["--version", version])
        
        try:
            result = self._run_command(args)
            
            if result.returncode == 0:
                return result.stdout
            
            raise Exception(result.stderr)
        
        except Exception as e:
            raise Exception(f"Template chart failed: {str(e)}")
    
    def get_release_values(
        self,
        name: str,
        namespace: Optional[str] = None,
        all_values: bool = False
    ) -> Dict[str, Any]:
        """Get values for a release.
        
        Args:
            name: Release name.
            namespace: Release namespace.
            all_values: Include defaults.
            
        Returns:
            Release values.
        """
        args = ["get", "values", name]
        
        if namespace:
            args.extend(["--namespace", namespace])
        
        if all_values:
            args.append("--all")
        
        try:
            result = self._run_command(args)
            
            if result.returncode != 0:
                return {}
            
            import json
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                return {}
        
        except Exception:
            return {}
    
    def history_release(
        self,
        name: str,
        namespace: Optional[str] = None,
        max_: int = 0
    ) -> List[Dict[str, Any]]:
        """Get release history.
        
        Args:
            name: Release name.
            namespace: Release namespace.
            max_: Maximum revisions to return.
            
        Returns:
            Release history.
        """
        args = ["history", name, "--output", "json"]
        
        if namespace:
            args.extend(["--namespace", namespace])
        
        if max_ > 0:
            args.extend(["--max", str(max_)])
        
        try:
            result = self._run_command(args)
            
            if result.returncode != 0:
                return []
            
            import json
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError:
                return []
        
        except Exception:
            return []


class HelmAction(BaseAction):
    """Helm action for Kubernetes package management.
    
    Supports chart management, release operations, and repository control.
    """
    action_type: str = "helm"
    display_name: str = "Helm动作"
    description: str = "Helm Kubernetes包管理器操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[HelmClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Helm operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "list_releases":
                return self._list_releases(params, start_time)
            elif operation == "get_release":
                return self._get_release(params, start_time)
            elif operation == "install":
                return self._install(params, start_time)
            elif operation == "upgrade":
                return self._upgrade(params, start_time)
            elif operation == "rollback":
                return self._rollback(params, start_time)
            elif operation == "uninstall":
                return self._uninstall(params, start_time)
            elif operation == "pull":
                return self._pull(params, start_time)
            elif operation == "search":
                return self._search(params, start_time)
            elif operation == "add_repo":
                return self._add_repo(params, start_time)
            elif operation == "update_repos":
                return self._update_repos(start_time)
            elif operation == "list_repos":
                return self._list_repos(start_time)
            elif operation == "template":
                return self._template(params, start_time)
            elif operation == "get_values":
                return self._get_values(params, start_time)
            elif operation == "history":
                return self._history(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Helm operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Initialize Helm client."""
        kubeconfig = params.get("kubeconfig")
        context = params.get("context")
        
        self._client = HelmClient(kubeconfig=kubeconfig, context=context)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message="Helm is available" if success else "Helm not available",
            duration=time.time() - start_time
        )
    
    def _list_releases(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List Helm releases."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            releases = self._client.list_releases(
                namespace=params.get("namespace"),
                all_namespaces=params.get("all_namespaces", False),
                status=params.get("status")
            )
            return ActionResult(success=True, message=f"Found {len(releases)} releases", data={"releases": releases}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_release(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get release information."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            release = self._client.get_release(name, params.get("namespace"))
            return ActionResult(success=release is not None, message=f"Found release: {name}" if release else f"Release not found: {name}", data={"release": release}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _install(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Install a Helm release."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        chart = params.get("chart", "")
        
        if not name or not chart:
            return ActionResult(success=False, message="name and chart are required", duration=time.time() - start_time)
        
        try:
            success = self._client.install_release(
                name=name,
                chart=chart,
                namespace=params.get("namespace"),
                values=params.get("values"),
                version=params.get("version"),
                wait=params.get("wait", True),
                timeout=params.get("timeout", 300)
            )
            return ActionResult(success=success, message=f"Release installed: {name}" if success else "Install failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _upgrade(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Upgrade a Helm release."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        chart = params.get("chart", "")
        
        if not name or not chart:
            return ActionResult(success=False, message="name and chart are required", duration=time.time() - start_time)
        
        try:
            success = self._client.upgrade_release(
                name=name,
                chart=chart,
                namespace=params.get("namespace"),
                values=params.get("values"),
                version=params.get("version"),
                wait=params.get("wait", True),
                timeout=params.get("timeout", 300)
            )
            return ActionResult(success=success, message=f"Release upgraded: {name}" if success else "Upgrade failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _rollback(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Rollback a Helm release."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.rollback_release(
                name=name,
                revision=params.get("revision"),
                namespace=params.get("namespace"),
                wait=params.get("wait", True)
            )
            return ActionResult(success=success, message=f"Release rolled back: {name}" if success else "Rollback failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _uninstall(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Uninstall a Helm release."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.uninstall_release(
                name=name,
                namespace=params.get("namespace"),
                keep_history=params.get("keep_history", False)
            )
            return ActionResult(success=success, message=f"Release uninstalled: {name}" if success else "Uninstall failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _pull(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Pull a chart from repository."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        chart = params.get("chart", "")
        if not chart:
            return ActionResult(success=False, message="chart is required", duration=time.time() - start_time)
        
        try:
            success = self._client.pull_chart(
                chart=chart,
                destination=params.get("destination", "."),
                version=params.get("version"),
                untar=params.get("untar", True)
            )
            return ActionResult(success=success, message=f"Chart pulled: {chart}" if success else "Pull failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _search(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Search for charts."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        query = params.get("query", "")
        if not query:
            return ActionResult(success=False, message="query is required", duration=time.time() - start_time)
        
        try:
            charts = self._client.search_repo(query, params.get("versions", False))
            return ActionResult(success=True, message=f"Found {len(charts)} charts", data={"charts": charts}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _add_repo(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a chart repository."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        url = params.get("url", "")
        
        if not name or not url:
            return ActionResult(success=False, message="name and url are required", duration=time.time() - start_time)
        
        try:
            success = self._client.add_repository(name, url)
            return ActionResult(success=success, message=f"Repository added: {name}" if success else "Add repo failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _update_repos(self, start_time: float) -> ActionResult:
        """Update chart repositories."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            success = self._client.update_repositories()
            return ActionResult(success=success, message="Repositories updated" if success else "Update repos failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_repos(self, start_time: float) -> ActionResult:
        """List chart repositories."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            repos = self._client.list_repositories()
            return ActionResult(success=True, message=f"Found {len(repos)} repositories", data={"repositories": repos}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _template(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Render chart templates."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        chart = params.get("chart", "")
        
        if not name or not chart:
            return ActionResult(success=False, message="name and chart are required", duration=time.time() - start_time)
        
        try:
            output = self._client.template_chart(
                name=name,
                chart=chart,
                namespace=params.get("namespace"),
                values=params.get("values"),
                version=params.get("version")
            )
            return ActionResult(success=True, message=f"Chart templated ({len(output)} chars)", data={"output": output}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_values(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get release values."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            values = self._client.get_release_values(name, params.get("namespace"), params.get("all_values", False))
            return ActionResult(success=True, message="Values retrieved", data={"values": values}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get release history."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            history = self._client.history_release(name, params.get("namespace"), params.get("max", 0))
            return ActionResult(success=True, message=f"Found {len(history)} revisions", data={"history": history}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
