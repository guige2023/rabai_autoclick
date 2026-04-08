"""Prefect action module for RabAI AutoClick.

Provides Prefect operations for
workflow orchestration and data pipeline management.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PrefectClient:
    """Prefect client for workflow orchestration.
    
    Provides methods for managing Prefect flows,
    deployments, and flow runs.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 4200,
        token: str = ""
    ) -> None:
        """Initialize Prefect client.
        
        Args:
            host: Prefect server host.
            port: Prefect server port.
            token: API token.
        """
        self.host = host
        self.port = port
        self.token = token
        self.base_url = f"http://{host}:{port}"
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to Prefect server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required")
        
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        
        try:
            self._session = requests.Session()
            self._session.headers.update(headers)
            
            response = self._session.get(
                f"{self.base_url}/api/health",
                timeout=30
            )
            
            return response.status_code == 200
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Prefect session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None
    ) -> Any:
        """Make a request to Prefect API."""
        if not self._session:
            raise RuntimeError("Not connected to Prefect")
        
        url = f"{self.base_url}{path}"
        
        try:
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception as e:
            raise Exception(f"Prefect request failed: {str(e)}")
    
    def get_flows(self) -> List[Dict[str, Any]]:
        """Get all flows.
        
        Returns:
            List of flows.
        """
        try:
            result = self._request("GET", "/api/flows")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_flow(self, flow_id: str) -> Optional[Dict[str, Any]]:
        """Get a flow.
        
        Args:
            flow_id: Flow ID.
            
        Returns:
            Flow information or None.
        """
        try:
            result = self._request("GET", f"/api/flows/{flow_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def create_flow(
        self,
        name: str,
        tags: Optional[List[str]] = None
    ) -> Optional[str]:
        """Create a flow.
        
        Args:
            name: Flow name.
            tags: Optional tags.
            
        Returns:
            Flow ID or None.
        """
        try:
            data: Dict[str, Any] = {"name": name}
            
            if tags:
                data["tags"] = tags
            
            result = self._request("POST", "/api/flows/", data=data)
            
            if isinstance(result, dict):
                return result.get("id")
            
            return None
        
        except Exception:
            return None
    
    def delete_flow(self, flow_id: str) -> bool:
        """Delete a flow.
        
        Args:
            flow_id: Flow ID to delete.
            
        Returns:
            True if deletion succeeded.
        """
        try:
            result = self._request("DELETE", f"/api/flows/{flow_id}")
            return result is True or result is None
        except Exception:
            return False
    
    def get_flow_runs(
        self,
        flow_id: Optional[str] = None,
        state: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get flow runs.
        
        Args:
            flow_id: Optional flow ID filter.
            state: Optional state filter.
            limit: Maximum results.
            
        Returns:
            List of flow runs.
        """
        try:
            params: Dict[str, Any] = {"limit": limit}
            
            if flow_id:
                params["flow_id"] = flow_id
            
            if state:
                params["state"] = state
            
            result = self._request("GET", "/api/flow_runs/", params)
            return result if isinstance(result, list) else []
        
        except Exception:
            return []
    
    def get_flow_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a flow run.
        
        Args:
            run_id: Flow run ID.
            
        Returns:
            Flow run information or None.
        """
        try:
            result = self._request("GET", f"/api/flow_runs/{run_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def create_flow_run(
        self,
        flow_id: str,
        parameters: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Create a flow run.
        
        Args:
            flow_id: Flow ID.
            parameters: Optional parameters.
            context: Optional context.
            
        Returns:
            Flow run ID or None.
        """
        try:
            data: Dict[str, Any] = {"flow_id": flow_id}
            
            if parameters:
                data["parameters"] = parameters
            
            if context:
                data["context"] = context
            
            result = self._request("POST", "/api/flow_runs/", data=data)
            
            if isinstance(result, dict):
                return result.get("id")
            
            return None
        
        except Exception:
            return None
    
    def update_flow_run(
        self,
        run_id: str,
        state: Optional[str] = None
    ) -> bool:
        """Update a flow run.
        
        Args:
            run_id: Flow run ID.
            state: New state.
            
        Returns:
            True if update succeeded.
        """
        try:
            data: Dict[str, Any] = {}
            
            if state:
                data["state"] = state
            
            result = self._request("PATCH", f"/api/flow_runs/{run_id}", data=data)
            return result is not None
        except Exception:
            return False
    
    def get_deployments(self) -> List[Dict[str, Any]]:
        """Get all deployments.
        
        Returns:
            List of deployments.
        """
        try:
            result = self._request("GET", "/api/deployments/")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_deployment(self, deployment_id: str) -> Optional[Dict[str, Any]]:
        """Get a deployment.
        
        Args:
            deployment_id: Deployment ID.
            
        Returns:
            Deployment information or None.
        """
        try:
            result = self._request("GET", f"/api/deployments/{deployment_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def create_deployment(
        self,
        name: str,
        flow_id: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Create a deployment.
        
        Args:
            name: Deployment name.
            flow_id: Flow ID.
            parameters: Optional parameters.
            
        Returns:
            Deployment ID or None.
        """
        try:
            data: Dict[str, Any] = {
                "name": name,
                "flow_id": flow_id
            }
            
            if parameters:
                data["parameters"] = parameters
            
            result = self._request("POST", "/api/deployments/", data=data)
            
            if isinstance(result, dict):
                return result.get("id")
            
            return None
        
        except Exception:
            return None
    
    def run_deployment(
        self,
        deployment_id: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Run a deployment.
        
        Args:
            deployment_id: Deployment ID.
            parameters: Optional parameters.
            
        Returns:
            Flow run ID or None.
        """
        try:
            data: Dict[str, Any] = {}
            
            if parameters:
                data["parameters"] = parameters
            
            result = self._request(
                "POST",
                f"/api/deployments/{deployment_id}/run",
                data=data
            )
            
            if isinstance(result, dict):
                return result.get("id")
            
            return None
        
        except Exception:
            return None
    
    def get_tasks(self, flow_run_id: str) -> List[Dict[str, Any]]:
        """Get tasks for a flow run.
        
        Args:
            flow_run_id: Flow run ID.
            
        Returns:
            List of tasks.
        """
        try:
            result = self._request(
                "GET",
                "/api/task_runs/",
                {"flow_run_id": flow_run_id}
            )
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_task_run(self, task_run_id: str) -> Optional[Dict[str, Any]]:
        """Get a task run.
        
        Args:
            task_run_id: Task run ID.
            
        Returns:
            Task run information or None.
        """
        try:
            result = self._request("GET", f"/api/task_runs/{task_run_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def get_work_queues(self) -> List[Dict[str, Any]]:
        """Get all work queues.
        
        Returns:
            List of work queues.
        """
        try:
            result = self._request("GET", "/api/work_queues/")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_work_queue(self, queue_id: str) -> Optional[Dict[str, Any]]:
        """Get a work queue.
        
        Args:
            queue_id: Work queue ID.
            
        Returns:
            Work queue information or None.
        """
        try:
            result = self._request("GET", f"/api/work_queues/{queue_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def get_logs_for_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Get logs for a flow run.
        
        Args:
            run_id: Flow run ID.
            
        Returns:
            List of log entries.
        """
        try:
            result = self._request(
                "GET",
                "/api/logs/",
                {"flow_run_id": run_id}
            )
            return result if isinstance(result, list) else []
        except Exception:
            return []


class PrefectAction(BaseAction):
    """Prefect action for workflow orchestration.
    
    Supports flow management, deployments, and run control.
    """
    action_type: str = "prefect"
    display_name: str = "Prefect动作"
    description: str = "Prefect工作流编排和数据管道管理"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[PrefectClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Prefect operation.
        
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
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "get_flows":
                return self._get_flows(start_time)
            elif operation == "get_flow":
                return self._get_flow(params, start_time)
            elif operation == "create_flow":
                return self._create_flow(params, start_time)
            elif operation == "delete_flow":
                return self._delete_flow(params, start_time)
            elif operation == "get_runs":
                return self._get_runs(params, start_time)
            elif operation == "get_run":
                return self._get_run(params, start_time)
            elif operation == "create_run":
                return self._create_run(params, start_time)
            elif operation == "update_run":
                return self._update_run(params, start_time)
            elif operation == "get_deployments":
                return self._get_deployments(start_time)
            elif operation == "get_deployment":
                return self._get_deployment(params, start_time)
            elif operation == "create_deployment":
                return self._create_deployment(params, start_time)
            elif operation == "run_deployment":
                return self._run_deployment(params, start_time)
            elif operation == "get_tasks":
                return self._get_tasks(params, start_time)
            elif operation == "get_task":
                return self._get_task(params, start_time)
            elif operation == "get_work_queues":
                return self._get_work_queues(start_time)
            elif operation == "get_work_queue":
                return self._get_work_queue(params, start_time)
            elif operation == "get_logs":
                return self._get_logs(params, start_time)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}", duration=time.time() - start_time)
        
        except ImportError as e:
            return ActionResult(success=False, message=f"Import error: {str(e)}", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=f"Prefect operation failed: {str(e)}", duration=time.time() - start_time)
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Prefect."""
        host = params.get("host", "localhost")
        port = params.get("port", 4200)
        token = params.get("token", "")
        
        self._client = PrefectClient(host=host, port=port, token=token)
        success = self._client.connect()
        
        return ActionResult(success=success, message=f"Connected to Prefect at {host}:{port}" if success else "Failed to connect", duration=time.time() - start_time)
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Prefect."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(success=True, message="Disconnected from Prefect", duration=time.time() - start_time)
    
    def _get_flows(self, start_time: float) -> ActionResult:
        """Get all flows."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            flows = self._client.get_flows()
            return ActionResult(success=True, message=f"Found {len(flows)} flows", data={"flows": flows}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_flow(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a flow."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        flow_id = params.get("flow_id", "")
        if not flow_id:
            return ActionResult(success=False, message="flow_id is required", duration=time.time() - start_time)
        
        try:
            flow = self._client.get_flow(flow_id)
            return ActionResult(success=flow is not None, message=f"Flow retrieved: {flow_id}", data={"flow": flow}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_flow(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a flow."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="name is required", duration=time.time() - start_time)
        
        try:
            flow_id = self._client.create_flow(name, params.get("tags"))
            return ActionResult(success=flow_id is not None, message=f"Flow created: {flow_id}" if flow_id else "Create failed", data={"flow_id": flow_id}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_flow(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a flow."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        flow_id = params.get("flow_id", "")
        if not flow_id:
            return ActionResult(success=False, message="flow_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_flow(flow_id)
            return ActionResult(success=success, message=f"Flow deleted: {flow_id}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_runs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get flow runs."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            runs = self._client.get_flow_runs(
                flow_id=params.get("flow_id"),
                state=params.get("state"),
                limit=params.get("limit", 100)
            )
            return ActionResult(success=True, message=f"Found {len(runs)} runs", data={"runs": runs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_run(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a flow run."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        run_id = params.get("run_id", "")
        if not run_id:
            return ActionResult(success=False, message="run_id is required", duration=time.time() - start_time)
        
        try:
            run = self._client.get_flow_run(run_id)
            return ActionResult(success=run is not None, message=f"Run retrieved: {run_id}", data={"run": run}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_run(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a flow run."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        flow_id = params.get("flow_id", "")
        if not flow_id:
            return ActionResult(success=False, message="flow_id is required", duration=time.time() - start_time)
        
        try:
            run_id = self._client.create_flow_run(
                flow_id,
                parameters=params.get("parameters"),
                context=params.get("context")
            )
            return ActionResult(success=run_id is not None, message=f"Run created: {run_id}" if run_id else "Create failed", data={"run_id": run_id}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _update_run(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Update a flow run."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        run_id = params.get("run_id", "")
        if not run_id:
            return ActionResult(success=False, message="run_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.update_flow_run(run_id, params.get("state"))
            return ActionResult(success=success, message=f"Run updated: {run_id}" if success else "Update failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_deployments(self, start_time: float) -> ActionResult:
        """Get deployments."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            deployments = self._client.get_deployments()
            return ActionResult(success=True, message=f"Found {len(deployments)} deployments", data={"deployments": deployments}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_deployment(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a deployment."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        deployment_id = params.get("deployment_id", "")
        if not deployment_id:
            return ActionResult(success=False, message="deployment_id is required", duration=time.time() - start_time)
        
        try:
            deployment = self._client.get_deployment(deployment_id)
            return ActionResult(success=deployment is not None, message=f"Deployment retrieved: {deployment_id}", data={"deployment": deployment}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_deployment(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a deployment."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        name = params.get("name", "")
        flow_id = params.get("flow_id", "")
        
        if not name or not flow_id:
            return ActionResult(success=False, message="name and flow_id are required", duration=time.time() - start_time)
        
        try:
            deployment_id = self._client.create_deployment(name, flow_id, params.get("parameters"))
            return ActionResult(success=deployment_id is not None, message=f"Deployment created: {deployment_id}" if deployment_id else "Create failed", data={"deployment_id": deployment_id}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _run_deployment(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Run a deployment."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        deployment_id = params.get("deployment_id", "")
        if not deployment_id:
            return ActionResult(success=False, message="deployment_id is required", duration=time.time() - start_time)
        
        try:
            run_id = self._client.run_deployment(deployment_id, params.get("parameters"))
            return ActionResult(success=run_id is not None, message=f"Deployment run started: {run_id}" if run_id else "Run failed", data={"run_id": run_id}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_tasks(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get tasks for a flow run."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        run_id = params.get("run_id", "")
        if not run_id:
            return ActionResult(success=False, message="run_id is required", duration=time.time() - start_time)
        
        try:
            tasks = self._client.get_tasks(run_id)
            return ActionResult(success=True, message=f"Found {len(tasks)} tasks", data={"tasks": tasks}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_task(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a task run."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        task_run_id = params.get("task_run_id", "")
        if not task_run_id:
            return ActionResult(success=False, message="task_run_id is required", duration=time.time() - start_time)
        
        try:
            task = self._client.get_task_run(task_run_id)
            return ActionResult(success=task is not None, message=f"Task run retrieved: {task_run_id}", data={"task": task}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_work_queues(self, start_time: float) -> ActionResult:
        """Get work queues."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            queues = self._client.get_work_queues()
            return ActionResult(success=True, message=f"Found {len(queues)} work queues", data={"queues": queues}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_work_queue(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a work queue."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queue_id = params.get("queue_id", "")
        if not queue_id:
            return ActionResult(success=False, message="queue_id is required", duration=time.time() - start_time)
        
        try:
            queue = self._client.get_work_queue(queue_id)
            return ActionResult(success=queue is not None, message=f"Work queue retrieved: {queue_id}", data={"queue": queue}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get logs for a flow run."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        run_id = params.get("run_id", "")
        if not run_id:
            return ActionResult(success=False, message="run_id is required", duration=time.time() - start_time)
        
        try:
            logs = self._client.get_logs_for_run(run_id)
            return ActionResult(success=True, message=f"Found {len(logs)} log entries", data={"logs": logs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
