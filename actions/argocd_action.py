"""ArgoCD action module for RabAI AutoClick.

Provides ArgoCD operations for
GitOps continuous delivery and application management.
"""

import os
import sys
import time
import subprocess
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ArgoCDClient:
    """ArgoCD client for GitOps continuous delivery.
    
    Provides methods for managing ArgoCD applications,
    projects, and cluster connections.
    """
    
    def __init__(
        self,
        server: str = "localhost",
        port: int = 8080,
        auth_token: str = "",
        insecure: bool = False
    ) -> None:
        """Initialize ArgoCD client.
        
        Args:
            server: ArgoCD server address.
            port: ArgoCD server port.
            auth_token: Authentication token.
            insecure: Skip TLS verification.
        """
        self.server = server
        self.port = port
        self.auth_token = auth_token
        self.insecure = insecure
        self.base_url = f"http://{server}:{port}"
    
    def _run_argocd(self, args: List[str], timeout: int = 300) -> subprocess.CompletedProcess:
        """Run argocd CLI command.
        
        Args:
            args: Command arguments.
            timeout: Command timeout.
            
        Returns:
            CompletedProcess result.
        """
        cmd = ["argocd"] + args
        
        if self.auth_token:
            cmd.extend(["--auth-token", self.auth_token])
        
        if self.insecure:
            cmd.append("--insecure")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            return result
        except subprocess.TimeoutExpired:
            raise Exception(f"argocd command timed out after {timeout}s")
        except Exception as e:
            raise Exception(f"argocd command failed: {str(e)}")
    
    def connect(self) -> bool:
        """Test if ArgoCD CLI is available.
        
        Returns:
            True if argocd is available, False otherwise.
        """
        try:
            result = self._run_argocd(["version", "--client"], timeout=30)
            return result.returncode == 0
        except Exception:
            return False
    
    def login(self, username: str, password: str) -> bool:
        """Login to ArgoCD server.
        
        Args:
            username: Username.
            password: Password.
            
        Returns:
            True if login succeeded.
        """
        try:
            cmd = [
                "argocd",
                "login",
                f"{self.server}:{self.port}",
                "--username", username,
                "--password", password
            ]
            
            if self.insecure:
                cmd.append("--insecure")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                output = result.stdout + result.stderr
                for line in output.split("\n"):
                    if "Logged in" in line or "login successful" in line.lower():
                        return True
                return True
            
            return False
        
        except Exception:
            return False
    
    def get_apps(self, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get applications.
        
        Args:
            namespace: Filter by namespace.
            
        Returns:
            List of applications.
        """
        try:
            args = ["app", "list", "--output", "json"]
            
            if namespace:
                args.extend(["--namespace", namespace])
            
            result = self._run_argocd(args)
            
            if result.returncode == 0:
                import json
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return []
            
            return []
        
        except Exception:
            return []
    
    def get_app(self, name: str) -> Optional[Dict[str, Any]]:
        """Get application details.
        
        Args:
            name: Application name.
            
        Returns:
            Application information or None.
        """
        try:
            result = self._run_argocd(["app", "get", name, "--output", "json"])
            
            if result.returncode == 0:
                import json
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return None
            
            return None
        
        except Exception:
            return None
    
    def create_app(
        self,
        name: str,
        repo: str,
        path: str,
        dest_server: str,
        dest_namespace: str,
        revision: str = "HEAD"
    ) -> bool:
        """Create an application.
        
        Args:
            name: Application name.
            repo: Repository URL.
            path: Path in repository.
            dest_server: Destination cluster.
            dest_namespace: Destination namespace.
            revision: Git revision.
            
        Returns:
            True if creation succeeded.
        """
        try:
            args = [
                "app", "create", name,
                "--repo", repo,
                "--path", path,
                "--dest-server", dest_server,
                "--dest-namespace", dest_namespace,
                "--revision", revision
            ]
            
            result = self._run_argocd(args)
            return result.returncode == 0
        
        except Exception:
            return False
    
    def delete_app(self, name: str) -> bool:
        """Delete an application.
        
        Args:
            name: Application name.
            
        Returns:
            True if deletion succeeded.
        """
        try:
            result = self._run_argocd(["app", "delete", name, "--yes"])
            return result.returncode == 0
        except Exception:
            return False
    
    def sync_app(self, name: str, revision: Optional[str] = None) -> bool:
        """Sync an application.
        
        Args:
            name: Application name.
            revision: Specific revision to sync.
            
        Returns:
            True if sync succeeded.
        """
        try:
            args = ["app", "sync", name]
            
            if revision:
                args.extend(["--revision", revision])
            
            result = self._run_argocd(args)
            return result.returncode == 0
        
        except Exception:
            return False
    
    def rollback_app(self, name: str, revision: str) -> bool:
        """Rollback an application.
        
        Args:
            name: Application name.
            revision: Revision to rollback to.
            
        Returns:
            True if rollback succeeded.
        """
        try:
            result = self._run_argocd([
                "app", "rollback", name, "--revision", revision
            ])
            return result.returncode == 0
        except Exception:
            return False
    
    def set_app_params(
        self,
        name: str,
        auto_prune: bool = False,
        self_heal: bool = False,
        allow_empty: bool = False
    ) -> bool:
        """Set application parameters.
        
        Args:
            name: Application name.
            auto_prune: Enable automatic pruning.
            self_heal: Enable self-healing.
            allow_empty: Allow empty resources.
            
        Returns:
            True if update succeeded.
        """
        try:
            args = ["app", "set", name]
            
            if auto_prune:
                args.append("--auto-prune")
            
            if self_heal:
                args.append("--self-heal")
            
            if allow_empty:
                args.append("--allow-empty")
            
            result = self._run_argocd(args)
            return result.returncode == 0
        
        except Exception:
            return False
    
    def get_app_manifest(self, name: str) -> str:
        """Get application manifest.
        
        Args:
            name: Application name.
            
        Returns:
            Application manifest.
        """
        try:
            result = self._run_argocd([
                "app", "manifests", name, "--source", "live"
            ])
            
            if result.returncode == 0:
                return result.stdout
            
            return ""
        
        except Exception:
            return ""
    
    def get_app_resources(self, name: str) -> List[Dict[str, Any]]:
        """Get application resources.
        
        Args:
            name: Application name.
            
        Returns:
            List of resources.
        """
        try:
            result = self._run_argocd([
                "app", "resources", name, "--output", "json"
            ])
            
            if result.returncode == 0:
                import json
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return []
            
            return []
        
        except Exception:
            return []
    
    def get_proj(self, name: str) -> Optional[Dict[str, Any]]:
        """Get project details.
        
        Args:
            name: Project name.
            
        Returns:
            Project information or None.
        """
        try:
            result = self._run_argocd(["proj", "get", name, "--output", "json"])
            
            if result.returncode == 0:
                import json
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return None
            
            return None
        
        except Exception:
            return None
    
    def list_proj(self) -> List[Dict[str, Any]]:
        """List projects.
        
        Returns:
            List of projects.
        """
        try:
            result = self._run_argocd(["proj", "list", "--output", "json"])
            
            if result.returncode == 0:
                import json
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return []
            
            return []
        
        except Exception:
            return []
    
    def get_clusters(self) -> List[Dict[str, Any]]:
        """Get clusters.
        
        Returns:
            List of clusters.
        """
        try:
            result = self._run_argocd(["cluster", "list", "--output", "json"])
            
            if result.returncode == 0:
                import json
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return []
            
            return []
        
        except Exception:
            return []
    
    def get_repos(self) -> List[Dict[str, Any]]:
        """Get repositories.
        
        Returns:
            List of repositories.
        """
        try:
            result = self._run_argocd(["repo", "list", "--output", "json"])
            
            if result.returncode == 0:
                import json
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return []
            
            return []
        
        except Exception:
            return []
    
    def add_repo(
        self,
        repo_url: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssh_private_key: Optional[str] = None
    ) -> bool:
        """Add a repository.
        
        Args:
            repo_url: Repository URL.
            username: Optional username.
            password: Optional password.
            ssh_private_key: Optional SSH private key path.
            
        Returns:
            True if add succeeded.
        """
        try:
            args = ["repo", "add", repo_url]
            
            if username and password:
                args.extend(["--username", username, "--password", password])
            elif ssh_private_key:
                args.extend(["--ssh-private-key-path", ssh_private_key])
            
            result = self._run_argocd(args)
            return result.returncode == 0
        
        except Exception:
            return False
    
    def get_app_history(self, name: str) -> List[Dict[str, Any]]:
        """Get application history.
        
        Args:
            name: Application name.
            
        Returns:
            List of history entries.
        """
        try:
            result = self._run_argocd([
                "app", "history", name, "--output", "json"
            ])
            
            if result.returncode == 0:
                import json
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    return []
            
            return []
        
        except Exception:
            return []
    
    def get_app_logs(
        self,
        name: str,
        tail: int = 100,
        follow: bool = False
    ) -> str:
        """Get application logs.
        
        Args:
            name: Application name.
            tail: Number of lines to show.
            follow: Follow logs.
            
        Returns:
            Log output.
        """
        try:
            args = ["app", "logs", name, f"--tail={tail}"]
            
            if follow:
                args.append("--follow")
            
            result = self._run_argocd(args)
            
            if result.returncode == 0:
                return result.stdout
            
            return ""
        
        except Exception:
            return ""


class ArgoCDAction(BaseAction):
    """ArgoCD action for GitOps continuous delivery.
    
    Supports application management, sync, and rollback operations.
    """
    action_type: str = "argocd"
    display_name: str = "ArgoCD动作"
    description: str = "ArgoCD GitOps持续交付操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[ArgoCDClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute ArgoCD operation.
        
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
            elif operation == "login":
                return self._login(params, start_time)
            elif operation == "get_apps":
                return self._get_apps(params, start_time)
            elif operation == "get_app":
                return self._get_app(params, start_time)
            elif operation == "create_app":
                return self._create_app(params, start_time)
            elif operation == "delete_app":
                return self._delete_app(params, start_time)
            elif operation == "sync":
                return self._sync(params, start_time)
            elif operation == "rollback":
                return self._rollback(params, start_time)
            elif operation == "set_app":
                return self._set_app(params, start_time)
            elif operation == "get_manifest":
                return self._get_manifest(params, start_time)
            elif operation == "get_resources":
                return self._get_resources(params, start_time)
            elif operation == "get_proj":
                return self._get_proj(params, start_time)
            elif operation == "list_proj":
                return self._list_proj(start_time)
            elif operation == "get_clusters":
                return self._get_clusters(start_time)
            elif operation == "get_repos":
                return self._get_repos(start_time)
            elif operation == "add_repo":
                return self._add_repo(params, start_time)
            elif operation == "get_history":
                return self._get_history(params, start_time)
            elif operation == "get_logs":
                return self._get_logs(params, start_time)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}", duration=time.time() - start_time)
        
        except Exception as e:
            return ActionResult(success=False, message=f"ArgoCD operation failed: {str(e)}", duration=time.time() - start_time)
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Initialize ArgoCD client."""
        server = params.get("server", "localhost")
        port = params.get("port", 8080)
        auth_token = params.get("auth_token", "")
        insecure = params.get("insecure", False)
        
        self._client = ArgoCDClient(
            server=server,
            port=port,
            auth_token=auth_token,
            insecure=insecure
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message="ArgoCD CLI is available" if success else "ArgoCD CLI not available",
            duration=time.time() - start_time
        )
    
    def _login(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Login to ArgoCD."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        username = params.get("username", "")
        password = params.get("password", "")
        
        if not username or not password:
            return ActionResult(success=False, message="username and password are required", duration=time.time() - start_time)
        
        try:
            success = self._client.login(username, password)
            return ActionResult(success=success, message="Login successful" if success else "Login failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_apps(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get applications."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            apps = self._client.get_apps(params.get("namespace"))
            return ActionResult(success=True, message=f"Found {len(apps)} apps", data={"apps": apps}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_app(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get application details."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            app = self._client.get_app(name)
            return ActionResult(success=app is not None, message=f"App retrieved: {name}", data={"app": app}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_app(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create an application."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        repo = params.get("repo", "")
        path = params.get("path", "")
        dest_server = params.get("dest_server", "")
        dest_namespace = params.get("dest_namespace", "")
        
        if not name or not repo or not path or not dest_server or not dest_namespace:
            return ActionResult(success=False, message="name, repo, path, dest_server, and dest_namespace are required", duration=time.time() - start_time)
        
        try:
            success = self._client.create_app(
                name=name,
                repo=repo,
                path=path,
                dest_server=dest_server,
                dest_namespace=dest_namespace,
                revision=params.get("revision", "HEAD")
            )
            return ActionResult(success=success, message=f"App created: {name}" if success else "Create failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_app(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete an application."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_app(name)
            return ActionResult(success=success, message=f"App deleted: {name}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _sync(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Sync an application."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.sync_app(name, params.get("revision"))
            return ActionResult(success=success, message=f"App synced: {name}" if success else "Sync failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _rollback(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Rollback an application."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        revision = params.get("revision", "")
        
        if not name or not revision:
            return ActionResult(success=False, message="name and revision are required", duration=time.time() - start_time)
        
        try:
            success = self._client.rollback_app(name, revision)
            return ActionResult(success=success, message=f"App rolled back: {name}" if success else "Rollback failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _set_app(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Set application parameters."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.set_app_params(
                name=name,
                auto_prune=params.get("auto_prune", False),
                self_heal=params.get("self_heal", False),
                allow_empty=params.get("allow_empty", False)
            )
            return ActionResult(success=success, message=f"App updated: {name}" if success else "Update failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_manifest(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get application manifest."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            manifest = self._client.get_app_manifest(name)
            return ActionResult(success=bool(manifest), message=f"Manifest retrieved ({len(manifest)} chars)", data={"manifest": manifest}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_resources(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get application resources."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            resources = self._client.get_app_resources(name)
            return ActionResult(success=True, message=f"Found {len(resources)} resources", data={"resources": resources}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_proj(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get project details."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            proj = self._client.get_proj(name)
            return ActionResult(success=proj is not None, message=f"Project retrieved: {name}", data={"project": proj}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_proj(self, start_time: float) -> ActionResult:
        """List projects."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            projs = self._client.list_proj()
            return ActionResult(success=True, message=f"Found {len(projs)} projects", data={"projects": projs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_clusters(self, start_time: float) -> ActionResult:
        """Get clusters."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            clusters = self._client.get_clusters()
            return ActionResult(success=True, message=f"Found {len(clusters)} clusters", data={"clusters": clusters}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_repos(self, start_time: float) -> ActionResult:
        """Get repositories."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            repos = self._client.get_repos()
            return ActionResult(success=True, message=f"Found {len(repos)} repos", data={"repos": repos}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _add_repo(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a repository."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        repo_url = params.get("repo_url", "")
        if not repo_url:
            return ActionResult(success=False, message="repo_url is required", duration=time.time() - start_time)
        
        try:
            success = self._client.add_repo(
                repo_url,
                username=params.get("username"),
                password=params.get("password"),
                ssh_private_key=params.get("ssh_private_key")
            )
            return ActionResult(success=success, message=f"Repo added: {repo_url}" if success else "Add repo failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_history(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get application history."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            history = self._client.get_app_history(name)
            return ActionResult(success=True, message=f"Found {len(history)} history entries", data={"history": history}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get application logs."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            logs = self._client.get_app_logs(
                name,
                tail=params.get("tail", 100),
                follow=params.get("follow", False)
            )
            return ActionResult(success=True, message=f"Retrieved {len(logs)} chars of logs", data={"logs": logs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
