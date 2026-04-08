"""Nomad action module for RabAI AutoClick.

Provides HashiCorp Nomad operations for
workload orchestration and cluster management.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class NomadClient:
    """Nomad client for workload orchestration.
    
    Provides methods for managing Nomad jobs,
    deployments, and cluster operations.
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 4646,
        token: str = "",
        scheme: str = "http"
    ) -> None:
        """Initialize Nomad client.
        
        Args:
            host: Nomad server host.
            port: Nomad server port.
            token: ACL token.
            scheme: HTTP scheme.
        """
        self.host = host
        self.port = port
        self.token = token
        self.scheme = scheme
        self.base_url = f"{scheme}://{host}:{port}"
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to Nomad server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required")
        
        headers = {}
        if self.token:
            headers["X-Nomad-Token"] = self.token
        
        try:
            self._session = requests.Session()
            self._session.headers.update(headers)
            
            response = self._session.get(
                f"{self.base_url}/v1/status/leader",
                timeout=30
            )
            
            return response.status_code == 200
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Nomad session."""
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
        """Make a request to Nomad."""
        if not self._session:
            raise RuntimeError("Not connected to Nomad")
        
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
            raise Exception(f"Nomad request failed: {str(e)}")
    
    def get_jobs(self) -> List[Dict[str, Any]]:
        """Get all jobs.
        
        Returns:
            List of jobs.
        """
        try:
            result = self._request("GET", "/v1/jobs")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get a job.
        
        Args:
            job_id: Job ID.
            
        Returns:
            Job information or None.
        """
        try:
            result = self._request("GET", f"/v1/job/{job_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def get_job_allocations(self, job_id: str) -> List[Dict[str, Any]]:
        """Get allocations for a job.
        
        Args:
            job_id: Job ID.
            
        Returns:
            List of allocations.
        """
        try:
            result = self._request("GET", f"/v1/job/{job_id}/allocations")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def submit_job(
        self,
        job_spec: str,
        dry_run: bool = False
    ) -> Optional[str]:
        """Submit a job.
        
        Args:
            job_spec: Job specification HCL or JSON.
            dry_run: Validate without submitting.
            
        Returns:
            Job ID or None.
        """
        try:
            params = {"dry-run": "true"} if dry_run else {}
            
            result = self._request(
                "POST",
                "/v1/jobs",
                params=params,
                data=job_spec
            )
            
            if isinstance(result, dict):
                return result.get("JobID")
            
            return None
        
        except Exception:
            return None
    
    def delete_job(
        self,
        job_id: str,
        purge: bool = False
    ) -> bool:
        """Delete a job.
        
        Args:
            job_id: Job ID to delete.
            purge: Remove job from registry.
            
        Returns:
            True if deletion succeeded.
        """
        try:
            params = {"purge": str(purge).lower()}
            result = self._request("DELETE", f"/v1/job/{job_id}", params)
            return result is True
        except Exception:
            return False
    
    def stop_job(self, job_id: str, purge: bool = False) -> bool:
        """Stop a job.
        
        Args:
            job_id: Job ID to stop.
            purge: Remove job from registry.
            
        Returns:
            True if stop succeeded.
        """
        try:
            data = {"JobID": job_id, "Purge": purge}
            result = self._request("POST", f"/v1/job/{job_id}/stop", data=data)
            return result is True
        except Exception:
            return False
    
    def scale_job(
        self,
        job_id: str,
        group: str,
        count: int
    ) -> bool:
        """Scale a job group.
        
        Args:
            job_id: Job ID.
            group: Group name.
            count: Desired count.
            
        Returns:
            True if scale succeeded.
        """
        try:
            data = {"JobID": job_id, "Group": group, "Count": count}
            result = self._request("POST", f"/v1/job/{job_id}/scale", data=data)
            return result is True
        except Exception:
            return False
    
    def get_allocations(self) -> List[Dict[str, Any]]:
        """Get all allocations.
        
        Returns:
            List of allocations.
        """
        try:
            result = self._request("GET", "/v1/allocations")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_allocation(self, alloc_id: str) -> Optional[Dict[str, Any]]:
        """Get an allocation.
        
        Args:
            alloc_id: Allocation ID.
            
        Returns:
            Allocation information or None.
        """
        try:
            result = self._request("GET", f"/v1/allocation/{alloc_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def get_allocation_logs(
        self,
        alloc_id: str,
        task: Optional[str] = None,
        follow: bool = False
    ) -> str:
        """Get allocation logs.
        
        Args:
            alloc_id: Allocation ID.
            task: Task name.
            follow: Follow logs.
            
        Returns:
            Log output.
        """
        try:
            params: Dict[str, Any] = {"type": "logs", "plain": "true"}
            
            if task:
                params["task"] = task
            
            if follow:
                params["follow"] = "true"
            
            result = self._request("GET", f"/v1/client/allocation/{alloc_id}/logs", params)
            return result if isinstance(result, str) else ""
        
        except Exception:
            return ""
    
    def get_nodes(self) -> List[Dict[str, Any]]:
        """Get all nodes.
        
        Returns:
            List of nodes.
        """
        try:
            result = self._request("GET", "/v1/nodes")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get a node.
        
        Args:
            node_id: Node ID.
            
        Returns:
            Node information or None.
        """
        try:
            result = self._request("GET", f"/v1/node/{node_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def drain_node(self, node_id: str, enable: bool = True) -> bool:
        """Drain a node.
        
        Args:
            node_id: Node ID.
            enable: Enable or disable drain mode.
            
        Returns:
            True if drain mode changed.
        """
        try:
            data = {"NodeID": node_id, "Drain": enable}
            result = self._request("POST", f"/v1/node/{node_id}/drain", data=data)
            return result is True
        except Exception:
            return False
    
    def get_regions(self) -> List[str]:
        """Get available regions.
        
        Returns:
            List of region names.
        """
        try:
            result = self._request("GET", "/v1/regions")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_namespaces(self) -> List[Dict[str, Any]]:
        """Get all namespaces.
        
        Returns:
            List of namespaces.
        """
        try:
            result = self._request("GET", "/v1/namespaces")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_deployments(self) -> List[Dict[str, Any]]:
        """Get all deployments.
        
        Returns:
            List of deployments.
        """
        try:
            result = self._request("GET", "/v1/deployments")
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
            result = self._request("GET", f"/v1/deployment/{deployment_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None
    
    def get_evaluations(self) -> List[Dict[str, Any]]:
        """Get all evaluations.
        
        Returns:
            List of evaluations.
        """
        try:
            result = self._request("GET", "/v1/evaluations")
            return result if isinstance(result, list) else []
        except Exception:
            return []
    
    def get_evaluation(self, eval_id: str) -> Optional[Dict[str, Any]]:
        """Get an evaluation.
        
        Args:
            eval_id: Evaluation ID.
            
        Returns:
            Evaluation information or None.
        """
        try:
            result = self._request("GET", f"/v1/evaluation/{eval_id}")
            return result if isinstance(result, dict) else None
        except Exception:
            return None


class NomadAction(BaseAction):
    """Nomad action for workload orchestration.
    
    Supports job management, scaling, and cluster operations.
    """
    action_type: str = "nomad"
    display_name: str = "Nomad动作"
    description: str = "HashiCorp Nomad工作负载编排操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[NomadClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Nomad operation.
        
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
            elif operation == "get_jobs":
                return self._get_jobs(start_time)
            elif operation == "get_job":
                return self._get_job(params, start_time)
            elif operation == "get_job_allocations":
                return self._get_job_allocations(params, start_time)
            elif operation == "submit_job":
                return self._submit_job(params, start_time)
            elif operation == "delete_job":
                return self._delete_job(params, start_time)
            elif operation == "stop_job":
                return self._stop_job(params, start_time)
            elif operation == "scale_job":
                return self._scale_job(params, start_time)
            elif operation == "get_allocations":
                return self._get_allocations(start_time)
            elif operation == "get_allocation":
                return self._get_allocation(params, start_time)
            elif operation == "get_logs":
                return self._get_logs(params, start_time)
            elif operation == "get_nodes":
                return self._get_nodes(start_time)
            elif operation == "get_node":
                return self._get_node(params, start_time)
            elif operation == "drain_node":
                return self._drain_node(params, start_time)
            elif operation == "get_regions":
                return self._get_regions(start_time)
            elif operation == "get_namespaces":
                return self._get_namespaces(start_time)
            elif operation == "get_deployments":
                return self._get_deployments(start_time)
            elif operation == "get_deployment":
                return self._get_deployment(params, start_time)
            elif operation == "get_evaluations":
                return self._get_evaluations(start_time)
            elif operation == "get_evaluation":
                return self._get_evaluation(params, start_time)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}", duration=time.time() - start_time)
        
        except ImportError as e:
            return ActionResult(success=False, message=f"Import error: {str(e)}", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=f"Nomad operation failed: {str(e)}", duration=time.time() - start_time)
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Nomad."""
        host = params.get("host", "localhost")
        port = params.get("port", 4646)
        token = params.get("token", "")
        scheme = params.get("scheme", "http")
        
        self._client = NomadClient(host=host, port=port, token=token, scheme=scheme)
        success = self._client.connect()
        
        return ActionResult(success=success, message=f"Connected to Nomad at {host}:{port}" if success else "Failed to connect", duration=time.time() - start_time)
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Nomad."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(success=True, message="Disconnected from Nomad", duration=time.time() - start_time)
    
    def _get_jobs(self, start_time: float) -> ActionResult:
        """Get all jobs."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            jobs = self._client.get_jobs()
            return ActionResult(success=True, message=f"Found {len(jobs)} jobs", data={"jobs": jobs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_id = params.get("job_id", "")
        if not job_id:
            return ActionResult(success=False, message="job_id is required", duration=time.time() - start_time)
        
        try:
            job = self._client.get_job(job_id)
            return ActionResult(success=job is not None, message=f"Job retrieved: {job_id}", data={"job": job}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_job_allocations(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get job allocations."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_id = params.get("job_id", "")
        if not job_id:
            return ActionResult(success=False, message="job_id is required", duration=time.time() - start_time)
        
        try:
            allocs = self._client.get_job_allocations(job_id)
            return ActionResult(success=True, message=f"Found {len(allocs)} allocations", data={"allocations": allocs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _submit_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Submit a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_spec = params.get("job_spec", "")
        if not job_spec:
            return ActionResult(success=False, message="job_spec is required", duration=time.time() - start_time)
        
        try:
            job_id = self._client.submit_job(job_spec, params.get("dry_run", False))
            return ActionResult(success=job_id is not None, message=f"Job submitted: {job_id}" if job_id else "Submit failed", data={"job_id": job_id}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_id = params.get("job_id", "")
        if not job_id:
            return ActionResult(success=False, message="job_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_job(job_id, params.get("purge", False))
            return ActionResult(success=success, message=f"Job deleted: {job_id}" if success else "Delete failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _stop_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Stop a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_id = params.get("job_id", "")
        if not job_id:
            return ActionResult(success=False, message="job_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.stop_job(job_id, params.get("purge", False))
            return ActionResult(success=success, message=f"Job stopped: {job_id}" if success else "Stop failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _scale_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Scale a job group."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_id = params.get("job_id", "")
        group = params.get("group", "")
        count = params.get("count", 0)
        
        if not job_id or not group or count < 0:
            return ActionResult(success=False, message="job_id, group, and count are required", duration=time.time() - start_time)
        
        try:
            success = self._client.scale_job(job_id, group, count)
            return ActionResult(success=success, message=f"Job scaled: {job_id}/{group} -> {count}" if success else "Scale failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_allocations(self, start_time: float) -> ActionResult:
        """Get all allocations."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            allocs = self._client.get_allocations()
            return ActionResult(success=True, message=f"Found {len(allocs)} allocations", data={"allocations": allocs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_allocation(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get an allocation."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        alloc_id = params.get("alloc_id", "")
        if not alloc_id:
            return ActionResult(success=False, message="alloc_id is required", duration=time.time() - start_time)
        
        try:
            alloc = self._client.get_allocation(alloc_id)
            return ActionResult(success=alloc is not None, message=f"Allocation retrieved: {alloc_id}", data={"allocation": alloc}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_logs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get allocation logs."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        alloc_id = params.get("alloc_id", "")
        if not alloc_id:
            return ActionResult(success=False, message="alloc_id is required", duration=time.time() - start_time)
        
        try:
            logs = self._client.get_allocation_logs(alloc_id, params.get("task"), params.get("follow", False))
            return ActionResult(success=True, message=f"Retrieved {len(logs)} chars of logs", data={"logs": logs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_nodes(self, start_time: float) -> ActionResult:
        """Get all nodes."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            nodes = self._client.get_nodes()
            return ActionResult(success=True, message=f"Found {len(nodes)} nodes", data={"nodes": nodes}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_node(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a node."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        node_id = params.get("node_id", "")
        if not node_id:
            return ActionResult(success=False, message="node_id is required", duration=time.time() - start_time)
        
        try:
            node = self._client.get_node(node_id)
            return ActionResult(success=node is not None, message=f"Node retrieved: {node_id}", data={"node": node}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _drain_node(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Drain a node."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        node_id = params.get("node_id", "")
        if not node_id:
            return ActionResult(success=False, message="node_id is required", duration=time.time() - start_time)
        
        try:
            success = self._client.drain_node(node_id, params.get("enable", True))
            return ActionResult(success=success, message=f"Node drain mode changed: {node_id}" if success else "Drain failed", duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_regions(self, start_time: float) -> ActionResult:
        """Get regions."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            regions = self._client.get_regions()
            return ActionResult(success=True, message=f"Found {len(regions)} regions", data={"regions": regions}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_namespaces(self, start_time: float) -> ActionResult:
        """Get namespaces."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            namespaces = self._client.get_namespaces()
            return ActionResult(success=True, message=f"Found {len(namespaces)} namespaces", data={"namespaces": namespaces}, duration=time.time() - start_time)
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
    
    def _get_evaluations(self, start_time: float) -> ActionResult:
        """Get evaluations."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            evals = self._client.get_evaluations()
            return ActionResult(success=True, message=f"Found {len(evals)} evaluations", data={"evaluations": evals}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_evaluation(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get an evaluation."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        eval_id = params.get("eval_id", "")
        if not eval_id:
            return ActionResult(success=False, message="eval_id is required", duration=time.time() - start_time)
        
        try:
            evaluation = self._client.get_evaluation(eval_id)
            return ActionResult(success=evaluation is not None, message=f"Evaluation retrieved: {eval_id}", data={"evaluation": evaluation}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
