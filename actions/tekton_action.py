"""Tekton action module for RabAI AutoClick.

Provides Tekton Pipeline operations for
Kubernetes-native CI/CD and build automation.
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


class TektonClient:
    """Tekton client for Kubernetes-native CI/CD.
    
    Provides methods for managing Tekton tasks,
    pipelines, and pipeline runs.
    """
    
    def __init__(
        self,
        namespace: str = "default",
        kubeconfig: Optional[str] = None,
        context: Optional[str] = None
    ) -> None:
        """Initialize Tekton client.
        
        Args:
            namespace: Kubernetes namespace.
            kubeconfig: Path to kubeconfig.
            context: Kubernetes context.
        """
        self.namespace = namespace
        self.kubeconfig = kubeconfig
        self.context = context
    
    def _runkubectl(self, args: List[str], timeout: int = 300) -> subprocess.CompletedProcess:
        """Run kubectl command."""
        cmd = ["kubectl"] + args
        
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
            raise Exception(f"kubectl timed out after {timeout}s")
        except Exception as e:
            raise Exception(f"kubectl failed: {str(e)}")
    
    def connect(self) -> bool:
        """Test if kubectl is available.
        
        Returns:
            True if kubectl is available, False otherwise.
        """
        try:
            result = self._runkubectl(["cluster-info"], timeout=30)
            return result.returncode == 0
        except Exception:
            return False
    
    def get_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks.
        
        Returns:
            List of tasks.
        """
        try:
            result = self._runkubectl(
                ["get", "tasks", "-o", "json", "-n", self.namespace]
            )
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []
    
    def get_task(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a task.
        
        Args:
            name: Task name.
            
        Returns:
            Task information or None.
        """
        try:
            result = self._runkubectl(
                ["get", "task", name, "-o", "json", "-n", self.namespace]
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            
            return None
        
        except Exception:
            return None
    
    def apply_task(self, manifest: str) -> bool:
        """Apply a task manifest.
        
        Args:
            manifest: Task YAML manifest.
            
        Returns:
            True if apply succeeded.
        """
        try:
            result = self._runkubectl(
                ["apply", "-f", "-"],
                input_data=manifest
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def delete_task(self, name: str) -> bool:
        """Delete a task.
        
        Args:
            name: Task name.
            
        Returns:
            True if delete succeeded.
        """
        try:
            result = self._runkubectl(
                ["delete", "task", name, "-n", self.namespace]
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def get_task_runs(self, task_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get task runs.
        
        Args:
            task_name: Optional task name filter.
            
        Returns:
            List of task runs.
        """
        try:
            args = ["get", "taskruns", "-o", "json", "-n", self.namespace]
            
            if task_name:
                args.extend(["-l", f"task.name={task_name}"])
            
            result = self._runkubectl(args)
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []
    
    def get_task_run(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a task run.
        
        Args:
            name: Task run name.
            
        Returns:
            Task run information or None.
        """
        try:
            result = self._runkubectl(
                ["get", "taskrun", name, "-o", "json", "-n", self.namespace]
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            
            return None
        
        except Exception:
            return None
    
    def get_pipelines(self) -> List[Dict[str, Any]]:
        """Get all pipelines.
        
        Returns:
            List of pipelines.
        """
        try:
            result = self._runkubectl(
                ["get", "pipelines", "-o", "json", "-n", self.namespace]
            )
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []
    
    def get_pipeline(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a pipeline.
        
        Args:
            name: Pipeline name.
            
        Returns:
            Pipeline information or None.
        """
        try:
            result = self._runkubectl(
                ["get", "pipeline", name, "-o", "json", "-n", self.namespace]
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            
            return None
        
        except Exception:
            return None
    
    def apply_pipeline(self, manifest: str) -> bool:
        """Apply a pipeline manifest.
        
        Args:
            manifest: Pipeline YAML manifest.
            
        Returns:
            True if apply succeeded.
        """
        try:
            result = self._runkubectl(
                ["apply", "-f", "-"],
                input_data=manifest
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def delete_pipeline(self, name: str) -> bool:
        """Delete a pipeline.
        
        Args:
            name: Pipeline name.
            
        Returns:
            True if delete succeeded.
        """
        try:
            result = self._runkubectl(
                ["delete", "pipeline", name, "-n", self.namespace]
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def get_pipeline_runs(self, pipeline_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get pipeline runs.
        
        Args:
            pipeline_name: Optional pipeline name filter.
            
        Returns:
            List of pipeline runs.
        """
        try:
            args = ["get", "pipelineruns", "-o", "json", "-n", self.namespace]
            
            if pipeline_name:
                args.extend(["-l", f"pipeline.name={pipeline_name}"])
            
            result = self._runkubectl(args)
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []
    
    def get_pipeline_run(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a pipeline run.
        
        Args:
            name: Pipeline run name.
            
        Returns:
            Pipeline run information or None.
        """
        try:
            result = self._runkubectl(
                ["get", "pipelinerun", name, "-o", "json", "-n", self.namespace]
            )
            
            if result.returncode == 0:
                return json.loads(result.stdout)
            
            return None
        
        except Exception:
            return None
    
    def create_pipeline_run(
        self,
        name: str,
        pipeline_ref: str,
        params: Optional[Dict[str, Any]] = None,
        service_account: Optional[str] = None
    ) -> bool:
        """Create a pipeline run.
        
        Args:
            name: Pipeline run name.
            pipeline_ref: Pipeline reference.
            params: Optional parameters.
            service_account: Optional service account.
            
        Returns:
            True if creation succeeded.
        """
        manifest: Dict[str, Any] = {
            "apiVersion": "tekton.dev/v1beta1",
            "kind": "PipelineRun",
            "metadata": {"name": name},
            "spec": {"pipelineRef": {"name": pipeline_ref}}
        }
        
        if params:
            manifest["spec"]["params"] = [
                {"name": k, "value": v}
                for k, v in params.items()
            ]
        
        if service_account:
            manifest["spec"]["serviceAccountName"] = service_account
        
        try:
            result = self._runkubectl(
                ["apply", "-f", "-"],
                input_data=json.dumps(manifest)
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def delete_pipeline_run(self, name: str) -> bool:
        """Delete a pipeline run.
        
        Args:
            name: Pipeline run name.
            
        Returns:
            True if delete succeeded.
        """
        try:
            result = self._runkubectl(
                ["delete", "pipelinerun", name, "-n", self.namespace]
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def cancel_pipeline_run(self, name: str) -> bool:
        """Cancel a pipeline run.
        
        Args:
            name: Pipeline run name.
            
        Returns:
            True if cancel succeeded.
        """
        try:
            result = self._runkubectl(
                ["patch", "pipelinerun", name, "-n", self.namespace,
                 "-p", '{"spec":{"status":"PipelineRunCancelled"}}']
            )
            return result.returncode == 0
        except Exception:
            return False
    
    def get_task_run_logs(self, name: str) -> str:
        """Get logs for a task run.
        
        Args:
            name: Task run name.
            
        Returns:
            Task run logs.
        """
        try:
            result = self._runkubectl(
                ["logs", "taskrun/" + name, "-n", self.namespace]
            )
            return result.stdout if result.returncode == 0 else ""
        except Exception:
            return ""
    
    def get_pipeline_run_logs(self, name: str) -> str:
        """Get logs for a pipeline run.
        
        Args:
            name: Pipeline run name.
            
        Returns:
            Pipeline run logs.
        """
        try:
            result = self._runkubectl(
                ["logs", "pipelinerun/" + name, "-n", self.namespace]
            )
            return result.stdout if result.returncode == 0 else ""
        except Exception:
            return ""
    
    def get_taskruns_for_pipelinerun(self, pipelinerun_name: str) -> List[Dict[str, Any]]:
        """Get task runs for a pipeline run.
        
        Args:
            pipelinerun_name: Pipeline run name.
            
        Returns:
            List of task runs.
        """
        try:
            result = self._runkubectl(
                ["get", "taskruns", "-o", "json", "-n", self.namespace,
                 "-l", f"tekton.dev/pipelineRun={pipelinerun_name}"]
            )
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return data.get("items", [])
            
            return []
        
        except Exception:
            return []
    
    def describe_task(self, name: str) -> str:
        """Describe a task.
        
        Args:
            name: Task name.
            
        Returns:
            Task description.
        """
        try:
            result = self._runkubectl(
                ["describe", "task", name, "-n", self.namespace]
            )
            return result.stdout if result.returncode == 0 else ""
        except Exception:
            return ""
    
    def describe_pipeline(self, name: str) -> str:
        """Describe a pipeline.
        
        Args:
            name: Pipeline name.
            
        Returns:
            Pipeline description.
        """
        try:
            result = self._runkubectl(
                ["describe", "pipeline", name, "-n", self.namespace]
            )
            return result.stdout if result.returncode == 0 else ""
        except Exception:
            return ""


class TektonAction(BaseAction):
    """Tekton action for Kubernetes-native CI/CD.
    
    Supports task management, pipeline runs, and build operations.
    """
    action_type: str = "tekton"
    display_name: str = "Tekton动作"
    description: str = "Tekton Kubernetes原生CI/CD管道操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[TektonClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Tekton operation.
        
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
            elif operation == "get_tasks":
                return self._get_tasks(start_time)
            elif operation == "get_task":
                return self._get_task(params, start_time)
            elif operation == "apply_task":
                return self._apply_task(params, start_time)
            elif operation == "delete_task":
                return self._delete_task(params, start_time)
            elif operation == "get_task_runs":
                return self._get_task_runs(params, start_time)
            elif operation == "get_task_run":
                return self._get_task_run(params, start_time)
            elif operation == "get_pipelines":
                return self._get_pipelines(start_time)
            elif operation == "get_pipeline":
                return self._get_pipeline(params, start_time)
            elif operation == "apply_pipeline":
                return self._apply_pipeline(params, start_time)
            elif operation == "delete_pipeline":
                return self._delete_pipeline(params, start_time)
            elif operation == "get_pipeline_runs":
                return self._get_pipeline_runs(params, start_time)
            elif operation == "get_pipeline_run":
                return self._get_pipeline_run(params, start_time)
            elif operation == "create_pipeline_run":
                return self._create_pipeline_run(params, start_time)
            elif operation == "delete_pipeline_run":
                return self._delete_pipeline_run(params, start_time)
            elif operation == "cancel_pipeline_run":
                return self._cancel_pipeline_run(params, start_time)
            elif operation == "get_task_run_logs":
                return self._get_task_run_logs(params, start_time)
            elif operation == "get_pipeline_run_logs":
                return self._get_pipeline_run_logs(params, start_time)
            elif operation == "get_taskruns_for_pipelinerun":
                return self._get_taskruns_for_pipelinerun(params, start_time)
            elif operation == "describe_task":
                return self._describe_task(params, start_time)
            elif operation == "describe_pipeline":
                return self._describe_pipeline(params, start_time)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}", duration=time.time() - start_time)
        
        except Exception as e:
            return ActionResult(success=False, message=f"Tekton operation failed: {str(e)}", duration=time.time() - start_time)
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Initialize Tekton client."""
        namespace = params.get("namespace", "default")
        kubeconfig = params.get("kubeconfig")
        context = params.get("context")
        
        self._client = TektonClient(
            namespace=namespace,
            kubeconfig=kubeconfig,
            context=context
        )
        
        success = self._client.connect()
        
        return ActionResult(success=success, message="kubectl is available" if success else "kubectl not available", duration=time.time() - start_time)
    
    def _get_tasks(self, start_time: float) -> ActionResult:
        """Get all tasks."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            tasks = self._client.get_tasks()
            return ActionResult(success=True, message=f"Found {len(tasks)} tasks", data={"tasks": tasks}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a task."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            task = self._client.get_task(name)
            return ActionResult(success=task is not None, message=f"Task retrieved: {name}", data={"task": task}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _apply_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Apply a task manifest."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        manifest = params.get("manifest", "")
        if not manifest:
            return ActionResult(success=False, message="manifest is required", duration=time.time() - start_time)
        
        try:
            success = self._client.apply_task(manifest)
            return ActionResult(success=success, message="Task applied" if success else "Apply failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a task."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_task(name)
            return ActionResult(success=success, message=f"Task deleted: {name}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_task_runs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get task runs."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            runs = self._client.get_task_runs(params.get("task_name"))
            return ActionResult(success=True, message=f"Found {len(runs)} task runs", data={"task_runs": runs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_task_run(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a task run."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            run = self._client.get_task_run(name)
            return ActionResult(success=run is not None, message=f"Task run retrieved: {name}", data={"task_run": run}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_pipelines(self, start_time: float) -> ActionResult:
        """Get all pipelines."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            pipelines = self._client.get_pipelines()
            return ActionResult(success=True, message=f"Found {len(pipelines)} pipelines", data={"pipelines": pipelines}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_pipeline(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a pipeline."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            pipeline = self._client.get_pipeline(name)
            return ActionResult(success=pipeline is not None, message=f"Pipeline retrieved: {name}", data={"pipeline": pipeline}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _apply_pipeline(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Apply a pipeline manifest."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        manifest = params.get("manifest", "")
        if not manifest:
            return ActionResult(success=False, message="manifest is required", duration=time.time() - start_time)
        
        try:
            success = self._client.apply_pipeline(manifest)
            return ActionResult(success=success, message="Pipeline applied" if success else "Apply failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_pipeline(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a pipeline."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_pipeline(name)
            return ActionResult(success=success, message=f"Pipeline deleted: {name}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_pipeline_runs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get pipeline runs."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        try:
            runs = self._client.get_pipeline_runs(params.get("pipeline_name"))
            return ActionResult(success=True, message=f"Found {len(runs)} pipeline runs", data={"pipeline_runs": runs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_pipeline_run(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a pipeline run."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            run = self._client.get_pipeline_run(name)
            return ActionResult(success=run is not None, message=f"Pipeline run retrieved: {name}", data={"pipeline_run": run}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_pipeline_run(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a pipeline run."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        pipeline_ref = params.get("pipeline_ref", "")
        
        if not name or not pipeline_ref:
            return ActionResult(success=False, message="name and pipeline_ref are required", duration=time.time() - start_time)
        
        try:
            success = self._client.create_pipeline_run(
                name=name,
                pipeline_ref=pipeline_ref,
                params=params.get("params"),
                service_account=params.get("service_account")
            )
            return ActionResult(success=success, message=f"Pipeline run created: {name}" if success else "Create failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_pipeline_run(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a pipeline run."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_pipeline_run(name)
            return ActionResult(success=success, message=f"Pipeline run deleted: {name}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _cancel_pipeline_run(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Cancel a pipeline run."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.cancel_pipeline_run(name)
            return ActionResult(success=success, message=f"Pipeline run cancelled: {name}" if success else "Cancel failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_task_run_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get task run logs."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            logs = self._client.get_task_run_logs(name)
            return ActionResult(success=bool(logs), message=f"Retrieved {len(logs)} chars of logs", data={"logs": logs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_pipeline_run_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get pipeline run logs."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            logs = self._client.get_pipeline_run_logs(name)
            return ActionResult(success=bool(logs), message=f"Retrieved {len(logs)} chars of logs", data={"logs": logs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_taskruns_for_pipelinerun(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get task runs for a pipeline run."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            taskruns = self._client.get_taskruns_for_pipelinerun(name)
            return ActionResult(success=True, message=f"Found {len(taskruns)} task runs", data={"taskruns": taskruns}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _describe_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Describe a task."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            output = self._client.describe_task(name)
            return ActionResult(success=bool(output), message="Task described", data={"description": output}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _describe_pipeline(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Describe a pipeline."""
        if not self._client:
            return ActionResult(success=False, message="Not initialized", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            output = self._client.describe_pipeline(name)
            return ActionResult(success=bool(output), message="Pipeline described", data={"description": output}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
