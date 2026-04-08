"""kubectl action module for RabAI AutoClick.

Provides Kubernetes kubectl operations including
pod management, deployment control, and resource operations.
"""

import os
import sys
import time
import subprocess
import json
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class KubectlClient:
    """kubectl client for Kubernetes operations.
    
    Provides methods for managing Kubernetes resources
    through the kubectl command-line tool.
    """
    
    def __init__(
        self,
        kubeconfig: Optional[str] = None,
        context: Optional[str] = None,
        namespace: Optional[str] = None,
        kubectl_path: str = "kubectl"
    ) -> None:
        """Initialize kubectl client.
        
        Args:
            kubeconfig: Path to kubeconfig file.
            context: Kubernetes context to use.
            namespace: Default namespace.
            kubectl_path: Path to kubectl binary.
        """
        self.kubeconfig = kubeconfig
        self.context = context
        self.namespace = namespace
        self.kubectl_path = kubectl_path
    
    def _run_command(
        self,
        args: List[str],
        timeout: int = 300,
        input_data: Optional[str] = None
    ) -> subprocess.CompletedProcess:
        """Run a kubectl command.
        
        Args:
            args: Command arguments.
            timeout: Command timeout.
            input_data: Optional stdin input.
            
        Returns:
            CompletedProcess result.
        """
        cmd = [self.kubectl_path] + args
        
        env = os.environ.copy()
        if self.kubeconfig:
            env["KUBECONFIG"] = self.kubeconfig
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=env,
                input=input_data,
                timeout=timeout
            )
            return result
        except subprocess.TimeoutExpired:
            raise Exception(f"kubectl command timed out after {timeout}s")
        except Exception as e:
            raise Exception(f"kubectl command failed: {str(e)}")
    
    def connect(self) -> bool:
        """Test if kubectl is available and cluster is reachable.
        
        Returns:
            True if kubectl is available, False otherwise.
        """
        try:
            result = self._run_command(["version", "--client"], timeout=30)
            if result.returncode != 0:
                return False
            
            result = self._run_command(["cluster-info"], timeout=30)
            return result.returncode == 0
        except Exception:
            return False
    
    def get_pods(
        self,
        namespace: Optional[str] = None,
        all_namespaces: bool = False,
        labels: Optional[str] = None,
        output: str = "json"
    ) -> List[Dict[str, Any]]:
        """Get pods.
        
        Args:
            namespace: Specific namespace.
            all_namespaces: List across all namespaces.
            labels: Label selector.
            output: Output format.
            
        Returns:
            List of pods.
        """
        args = ["get", "pods", "-o", output]
        
        if all_namespaces:
            args.append("--all-namespaces")
        elif namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        if labels:
            args.extend(["-l", labels])
        
        try:
            result = self._run_command(args)
            
            if result.returncode != 0:
                return []
            
            if output == "json":
                data = json.loads(result.stdout)
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []
    
    def get_services(
        self,
        namespace: Optional[str] = None,
        output: str = "json"
    ) -> List[Dict[str, Any]]:
        """Get services.
        
        Args:
            namespace: Specific namespace.
            output: Output format.
            
        Returns:
            List of services.
        """
        args = ["get", "services", "-o", output]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        try:
            result = self._run_command(args)
            
            if result.returncode != 0:
                return []
            
            if output == "json":
                data = json.loads(result.stdout)
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []
    
    def get_deployments(
        self,
        namespace: Optional[str] = None,
        output: str = "json"
    ) -> List[Dict[str, Any]]:
        """Get deployments.
        
        Args:
            namespace: Specific namespace.
            output: Output format.
            
        Returns:
            List of deployments.
        """
        args = ["get", "deployments", "-o", output]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        try:
            result = self._run_command(args)
            
            if result.returncode != 0:
                return []
            
            if output == "json":
                data = json.loads(result.stdout)
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []
    
    def get_nodes(self, output: str = "json") -> List[Dict[str, Any]]:
        """Get nodes.
        
        Args:
            output: Output format.
            
        Returns:
            List of nodes.
        """
        args = ["get", "nodes", "-o", output]
        
        try:
            result = self._run_command(args)
            
            if result.returncode != 0:
                return []
            
            if output == "json":
                data = json.loads(result.stdout)
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []
    
    def describe_pod(
        self,
        name: str,
        namespace: Optional[str] = None
    ) -> str:
        """Describe a pod.
        
        Args:
            name: Pod name.
            namespace: Pod namespace.
            
        Returns:
            Pod description.
        """
        args = ["describe", "pod", name]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        try:
            result = self._run_command(args)
            
            if result.returncode == 0:
                return result.stdout
            
            return ""
        
        except Exception:
            return ""
    
    def logs(
        self,
        name: str,
        namespace: Optional[str] = None,
        container: Optional[str] = None,
        tail: int = 100,
        previous: bool = False
    ) -> str:
        """Get pod logs.
        
        Args:
            name: Pod name.
            namespace: Pod namespace.
            container: Container name.
            tail: Number of lines to show.
            previous: Get previous terminated container logs.
            
        Returns:
            Pod logs.
        """
        args = ["logs", name, f"--tail={tail}"]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        if container:
            args.extend(["-c", container])
        
        if previous:
            args.append("--previous")
        
        try:
            result = self._run_command(args)
            return result.stdout if result.returncode == 0 else ""
        
        except Exception:
            return ""
    
    def apply_manifest(
        self,
        manifest: str,
        namespace: Optional[str] = None,
        dry_run: bool = False
    ) -> bool:
        """Apply a manifest.
        
        Args:
            manifest: YAML manifest content.
            namespace: Target namespace.
            dry_run: Dry run mode.
            
        Returns:
            True if apply succeeded.
        """
        args = ["apply", "-f", "-"]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        if dry_run:
            args.append("--dry-run=client")
        
        try:
            result = self._run_command(args, input_data=manifest)
            return result.returncode == 0
        except Exception:
            return False
    
    def delete_resource(
        self,
        resource_type: str,
        name: str,
        namespace: Optional[str] = None,
        force: bool = False
    ) -> bool:
        """Delete a resource.
        
        Args:
            resource_type: Resource type (pod, service, deployment, etc.).
            name: Resource name.
            namespace: Resource namespace.
            force: Force deletion.
            
        Returns:
            True if delete succeeded.
        """
        args = ["delete", resource_type, name]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        if force:
            args.append("--force=grace-period=0")
        
        try:
            result = self._run_command(args)
            return result.returncode == 0
        except Exception:
            return False
    
    def scale_deployment(
        self,
        name: str,
        replicas: int,
        namespace: Optional[str] = None
    ) -> bool:
        """Scale a deployment.
        
        Args:
            name: Deployment name.
            replicas: Number of replicas.
            namespace: Deployment namespace.
            
        Returns:
            True if scale succeeded.
        """
        args = ["scale", f"deployment/{name}", f"--replicas={replicas}"]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        try:
            result = self._run_command(args)
            return result.returncode == 0
        except Exception:
            return False
    
    def rollout_status(
        self,
        resource_type: str,
        name: str,
        namespace: Optional[str] = None,
        watch: bool = True
    ) -> bool:
        """Check rollout status.
        
        Args:
            resource_type: Resource type (deployment, statefulset, etc.).
            name: Resource name.
            namespace: Resource namespace.
            watch: Watch the rollout.
            
        Returns:
            True if rollout succeeded.
        """
        args = ["rollout", "status", f"{resource_type}/{name}"]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        if not watch:
            args.append("--timeout=0s")
        
        try:
            result = self._run_command(args)
            return result.returncode == 0
        except Exception:
            return False
    
    def rollout_undo(
        self,
        resource_type: str,
        name: str,
        namespace: Optional[str] = None,
        to_revision: Optional[int] = None
    ) -> bool:
        """Undo a rollout.
        
        Args:
            resource_type: Resource type.
            name: Resource name.
            namespace: Resource namespace.
            to_revision: Specific revision to rollback to.
            
        Returns:
            True if undo succeeded.
        """
        args = ["rollout", "undo", f"{resource_type}/{name}"]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        if to_revision:
            args.extend(["--to-revision", str(to_revision)])
        
        try:
            result = self._run_command(args)
            return result.returncode == 0
        except Exception:
            return False
    
    def exec_command(
        self,
        pod: str,
        command: List[str],
        namespace: Optional[str] = None,
        container: Optional[str] = None
    ) -> str:
        """Execute a command in a pod.
        
        Args:
            pod: Pod name.
            command: Command to execute.
            namespace: Pod namespace.
            container: Container name.
            
        Returns:
            Command output.
        """
        args = ["exec", pod, "--"]
        args.extend(command)
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        if container:
            args.insert(2, "-c")
            args.insert(3, container)
        
        try:
            result = self._run_command(args, timeout=60)
            return result.stdout
        except Exception as e:
            raise Exception(f"Exec command failed: {str(e)}")
    
    def port_forward(
        self,
        pod: str,
        local_port: int,
        remote_port: int,
        namespace: Optional[str] = None
    ) -> bool:
        """Port forward to a pod.
        
        Args:
            pod: Pod name.
            local_port: Local port.
            remote_port: Remote port.
            namespace: Pod namespace.
            
        Returns:
            True if port forward succeeded.
        """
        args = ["port-forward", pod, f"{local_port}:{remote_port}"]
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        try:
            result = self._run_command(args, timeout=5)
            return result.returncode == 0
        except Exception:
            return False
    
    def get_resource(
        self,
        resource_type: str,
        name: Optional[str] = None,
        namespace: Optional[str] = None,
        output: str = "json"
    ) -> List[Dict[str, Any]]:
        """Get a resource.
        
        Args:
            resource_type: Resource type.
            name: Optional resource name.
            namespace: Optional namespace.
            output: Output format.
            
        Returns:
            Resource data.
        """
        args = ["get", resource_type, "-o", output]
        
        if name:
            args.append(name)
        
        if namespace or self.namespace:
            args.extend(["-n", namespace or self.namespace])
        
        try:
            result = self._run_command(args)
            
            if result.returncode != 0:
                return []
            
            if output == "json":
                data = json.loads(result.stdout)
                if name:
                    return [data]
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []


class KubectlAction(BaseAction):
    """kubectl action for Kubernetes operations.
    
    Supports pod management, deployment control, and resource operations.
    """
    action_type: str = "kubectl"
    display_name: str = "kubectl动作"
    description: str = "Kubernetes kubectl资源管理操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[KubectlClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute kubectl operation.
        
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
            elif operation == "get_pods":
                return self._get_pods(params, start_time)
            elif operation == "get_services":
                return self._get_services(params, start_time)
            elif operation == "get_deployments":
                return self._get_deployments(params, start_time)
            elif operation == "get_nodes":
                return self._get_nodes(start_time)
            elif operation == "describe":
                return self._describe(params, start_time)
            elif operation == "logs":
                return self._logs(params, start_time)
            elif operation == "apply":
                return self._apply(params, start_time)
            elif operation == "delete":
                return self._delete(params, start_time)
            elif operation == "scale":
                return self._scale(params, start_time)
            elif operation == "rollout_status":
                return self._rollout_status(params, start_time)
            elif operation == "rollout_undo":
                return self._rollout_undo(params, start_time)
            elif operation == "exec":
                return self._exec(params, start_time)
            elif operation == "get_resource":
                return self._get_resource(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"kubectl operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Initialize kubectl client."""
        kubeconfig = params.get("kubeconfig")
        context = params.get("context")
        namespace = params.get("namespace")
        
        self._client = KubectlClient(
            kubeconfig=kubeconfig,
            context=context,
            namespace=namespace
        )
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message="kubectl is available" if success else "kubectl not available",
            duration=time.time() - start_time
        )
    
    def _get_pods(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get pods."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            pods = self._client.get_pods(
                namespace=params.get("namespace"),
                all_namespaces=params.get("all_namespaces", False),
                labels=params.get("labels")
            )
            return ActionResult(success=True, message=f"Found {len(pods)} pods", data={"pods": pods, "count": len(pods)}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_services(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get services."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            services = self._client.get_services(namespace=params.get("namespace"))
            return ActionResult(success=True, message=f"Found {len(services)} services", data={"services": services, "count": len(services)}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_deployments(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get deployments."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            deployments = self._client.get_deployments(namespace=params.get("namespace"))
            return ActionResult(success=True, message=f"Found {len(deployments)} deployments", data={"deployments": deployments, "count": len(deployments)}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_nodes(self, start_time: float) -> ActionResult:
        """Get nodes."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            nodes = self._client.get_nodes()
            return ActionResult(success=True, message=f"Found {len(nodes)} nodes", data={"nodes": nodes, "count": len(nodes)}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _describe(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Describe a resource."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        resource_type = params.get("resource_type", "pod")
        name = params.get("name", "")
        
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            if resource_type == "pod":
                output = self._client.describe_pod(name, params.get("namespace"))
            else:
                output = ""
            
            return ActionResult(success=bool(output), message="Resource described", data={"output": output}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get pod logs."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            logs = self._client.logs(
                name=name,
                namespace=params.get("namespace"),
                container=params.get("container"),
                tail=params.get("tail", 100),
                previous=params.get("previous", False)
            )
            return ActionResult(success=True, message=f"Retrieved {len(logs)} chars of logs", data={"logs": logs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _apply(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Apply a manifest."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        manifest = params.get("manifest", "")
        if not manifest:
            return ActionResult(success=False, message="manifest is required", duration=time.time() - start_time)
        
        try:
            success = self._client.apply_manifest(
                manifest=manifest,
                namespace=params.get("namespace"),
                dry_run=params.get("dry_run", False)
            )
            return ActionResult(success=success, message="Manifest applied" if success else "Apply failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a resource."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        resource_type = params.get("resource_type", "")
        name = params.get("name", "")
        
        if not resource_type or not name:
            return ActionResult(success=False, message="resource_type and name are required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_resource(
                resource_type=resource_type,
                name=name,
                namespace=params.get("namespace"),
                force=params.get("force", False)
            )
            return ActionResult(success=success, message=f"Deleted {resource_type}/{name}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _scale(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Scale a deployment."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        replicas = params.get("replicas", 0)
        
        if not name or replicas < 0:
            return ActionResult(success=False, message="name and replicas are required", duration=time.time() - start_time)
        
        try:
            success = self._client.scale_deployment(name, replicas, params.get("namespace"))
            return ActionResult(success=success, message=f"Scaled {name} to {replicas} replicas" if success else "Scale failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _rollout_status(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Check rollout status."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        resource_type = params.get("resource_type", "deployment")
        name = params.get("name", "")
        
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.rollout_status(
                resource_type=resource_type,
                name=name,
                namespace=params.get("namespace")
            )
            return ActionResult(success=success, message="Rollout complete" if success else "Rollout failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _rollout_undo(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Undo a rollout."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        resource_type = params.get("resource_type", "deployment")
        name = params.get("name", "")
        
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.rollout_undo(
                resource_type=resource_type,
                name=name,
                namespace=params.get("namespace"),
                to_revision=params.get("to_revision")
            )
            return ActionResult(success=success, message=f"Rollback initiated for {name}" if success else "Rollback failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _exec(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a command in a pod."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        command = params.get("command", [])
        
        if not name or not command:
            return ActionResult(success=False, message="name and command are required", duration=time.time() - start_time)
        
        try:
            output = self._client.exec_command(
                pod=name,
                command=command,
                namespace=params.get("namespace"),
                container=params.get("container")
            )
            return ActionResult(success=True, message="Command executed", data={"output": output}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_resource(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a resource."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        resource_type = params.get("resource_type", "")
        if not resource_type:
            return ActionResult(success=False, message="resource_type is required", duration=time.time() - start_time)
        
        try:
            resources = self._client.get_resource(
                resource_type=resource_type,
                name=params.get("name"),
                namespace=params.get("namespace")
            )
            return ActionResult(success=True, message=f"Found {len(resources)} resources", data={"resources": resources, "count": len(resources)}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
