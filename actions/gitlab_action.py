"""GitLab action module for RabAI AutoClick.

Provides GitLab operations including
project management, CI/CD pipeline control, and merge request handling.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GitLabClient:
    """GitLab client for DevOps operations.
    
    Provides methods for managing GitLab projects,
    pipelines, and merge requests.
    """
    
    def __init__(
        self,
        url: str = "https://gitlab.com",
        private_token: str = ""
    ) -> None:
        """Initialize GitLab client.
        
        Args:
            url: GitLab server URL.
            private_token: Private token for authentication.
        """
        self.url = url.rstrip("/")
        self.private_token = private_token
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to GitLab API.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required. Install with: pip install requests")
        
        if not self.private_token:
            return False
        
        try:
            self._session = requests.Session()
            self._session.headers.update({
                "PRIVATE-TOKEN": self.private_token,
                "Content-Type": "application/json"
            })
            
            response = self._session.get(
                f"{self.url}/api/v4/user",
                timeout=30
            )
            
            return response.status_code == 200
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the GitLab session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def list_projects(self, membership: bool = True) -> List[Dict[str, Any]]:
        """List projects.
        
        Args:
            membership: Only show projects the user is a member of.
            
        Returns:
            List of project information.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            params = {"membership": membership} if membership else {}
            response = self._session.get(
                f"{self.url}/api/v4/projects",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List projects failed: {str(e)}")
    
    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        """Get a project by ID.
        
        Args:
            project_id: Project ID or path.
            
        Returns:
            Project information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.get(
                f"{self.url}/api/v4/projects/{encoded_id}",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def list_branches(self, project_id: str) -> List[Dict[str, Any]]:
        """List branches for a project.
        
        Args:
            project_id: Project ID or path.
            
        Returns:
            List of branch information.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.get(
                f"{self.url}/api/v4/projects/{encoded_id}/repository/branches",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List branches failed: {str(e)}")
    
    def create_branch(
        self,
        project_id: str,
        branch: str,
        ref: str
    ) -> Optional[Dict[str, Any]]:
        """Create a new branch.
        
        Args:
            project_id: Project ID or path.
            branch: New branch name.
            ref: Source branch or commit SHA.
            
        Returns:
            Branch information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.post(
                f"{self.url}/api/v4/projects/{encoded_id}/repository/branches",
                json={"branch": branch, "ref": ref},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def list_pipelines(
        self,
        project_id: str,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List pipelines for a project.
        
        Args:
            project_id: Project ID or path.
            status: Optional filter by status.
            
        Returns:
            List of pipeline information.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            params = {}
            if status:
                params["status"] = status
            
            response = self._session.get(
                f"{self.url}/api/v4/projects/{encoded_id}/pipelines",
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List pipelines failed: {str(e)}")
    
    def trigger_pipeline(
        self,
        project_id: str,
        ref: str,
        variables: Optional[Dict[str, str]] = None
    ) -> Optional[Dict[str, Any]]:
        """Trigger a new pipeline.
        
        Args:
            project_id: Project ID or path.
            ref: Git reference (branch, tag, commit).
            variables: Optional pipeline variables.
            
        Returns:
            Pipeline information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            data: Dict[str, Any] = {"ref": ref}
            if variables:
                data["variables"] = [
                    {"key": k, "value": v}
                    for k, v in variables.items()
                ]
            
            response = self._session.post(
                f"{self.url}/api/v4/projects/{encoded_id}/pipeline",
                json=data,
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def get_pipeline(self, project_id: str, pipeline_id: int) -> Optional[Dict[str, Any]]:
        """Get a pipeline by ID.
        
        Args:
            project_id: Project ID or path.
            pipeline_id: Pipeline ID.
            
        Returns:
            Pipeline information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.get(
                f"{self.url}/api/v4/projects/{encoded_id}/pipelines/{pipeline_id}",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def retry_pipeline(self, project_id: str, pipeline_id: int) -> Optional[Dict[str, Any]]:
        """Retry a failed pipeline.
        
        Args:
            project_id: Project ID or path.
            pipeline_id: Pipeline ID.
            
        Returns:
            New pipeline information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.post(
                f"{self.url}/api/v4/projects/{encoded_id}/pipelines/{pipeline_id}/retry",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def cancel_pipeline(self, project_id: str, pipeline_id: int) -> Optional[Dict[str, Any]]:
        """Cancel a running pipeline.
        
        Args:
            project_id: Project ID or path.
            pipeline_id: Pipeline ID.
            
        Returns:
            Pipeline information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.post(
                f"{self.url}/api/v4/projects/{encoded_id}/pipelines/{pipeline_id}/cancel",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def list_merge_requests(
        self,
        project_id: str,
        state: str = "opened"
    ) -> List[Dict[str, Any]]:
        """List merge requests.
        
        Args:
            project_id: Project ID or path.
            state: Filter by state (opened, closed, merged, all).
            
        Returns:
            List of merge request information.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.get(
                f"{self.url}/api/v4/projects/{encoded_id}/merge_requests",
                params={"state": state},
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List merge requests failed: {str(e)}")
    
    def get_merge_request(
        self,
        project_id: str,
        mr_iid: int
    ) -> Optional[Dict[str, Any]]:
        """Get a merge request by IID.
        
        Args:
            project_id: Project ID or path.
            mr_iid: Merge request IID.
            
        Returns:
            Merge request information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.get(
                f"{self.url}/api/v4/projects/{encoded_id}/merge_requests/{mr_iid}",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def merge_merge_request(
        self,
        project_id: str,
        mr_iid: int,
        squash: bool = False
    ) -> Optional[Dict[str, Any]]:
        """Merge a merge request.
        
        Args:
            project_id: Project ID or path.
            mr_iid: Merge request IID.
            squash: Squash commits.
            
        Returns:
            Merged MR information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.put(
                f"{self.url}/api/v4/projects/{encoded_id}/merge_requests/{mr_iid}/merge",
                json={"squash": squash},
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def create_merge_request(
        self,
        project_id: str,
        title: str,
        source_branch: str,
        target_branch: str = "main",
        description: str = ""
    ) -> Optional[Dict[str, Any]]:
        """Create a merge request.
        
        Args:
            project_id: Project ID or path.
            title: MR title.
            source_branch: Source branch.
            target_branch: Target branch.
            description: Optional description.
            
        Returns:
            MR information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.post(
                f"{self.url}/api/v4/projects/{encoded_id}/merge_requests",
                json={
                    "title": title,
                    "source_branch": source_branch,
                    "target_branch": target_branch,
                    "description": description
                },
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def list_jobs(self, project_id: str, pipeline_id: int) -> List[Dict[str, Any]]:
        """List jobs for a pipeline.
        
        Args:
            project_id: Project ID or path.
            pipeline_id: Pipeline ID.
            
        Returns:
            List of job information.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.get(
                f"{self.url}/api/v4/projects/{encoded_id}/pipelines/{pipeline_id}/jobs",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return []
        
        except Exception as e:
            raise Exception(f"List jobs failed: {str(e)}")
    
    def retry_job(self, project_id: str, job_id: int) -> Optional[Dict[str, Any]]:
        """Retry a failed job.
        
        Args:
            project_id: Project ID or path.
            job_id: Job ID.
            
        Returns:
            Job information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to GitLab")
        
        try:
            import urllib.parse
            encoded_id = urllib.parse.quote(project_id, safe="")
            
            response = self._session.post(
                f"{self.url}/api/v4/projects/{encoded_id}/jobs/{job_id}/retry",
                timeout=30
            )
            
            if response.status_code in (200, 201):
                return response.json()
            
            return None
        
        except Exception:
            return None


class GitLabAction(BaseAction):
    """GitLab action for DevOps operations.
    
    Supports project management, CI/CD pipelines, and merge requests.
    """
    action_type: str = "gitlab"
    display_name: str = "GitLab动作"
    description: str = "GitLab项目管理和CI/CD流水线操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[GitLabClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute GitLab operation.
        
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
            elif operation == "list_projects":
                return self._list_projects(start_time)
            elif operation == "get_project":
                return self._get_project(params, start_time)
            elif operation == "list_branches":
                return self._list_branches(params, start_time)
            elif operation == "create_branch":
                return self._create_branch(params, start_time)
            elif operation == "list_pipelines":
                return self._list_pipelines(params, start_time)
            elif operation == "trigger_pipeline":
                return self._trigger_pipeline(params, start_time)
            elif operation == "get_pipeline":
                return self._get_pipeline(params, start_time)
            elif operation == "retry_pipeline":
                return self._retry_pipeline(params, start_time)
            elif operation == "cancel_pipeline":
                return self._cancel_pipeline(params, start_time)
            elif operation == "list_merge_requests":
                return self._list_mrs(params, start_time)
            elif operation == "get_merge_request":
                return self._get_mr(params, start_time)
            elif operation == "merge_merge_request":
                return self._merge_mr(params, start_time)
            elif operation == "create_merge_request":
                return self._create_mr(params, start_time)
            elif operation == "list_jobs":
                return self._list_jobs(params, start_time)
            elif operation == "retry_job":
                return self._retry_job(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GitLab operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to GitLab."""
        url = params.get("url", "https://gitlab.com")
        private_token = params.get("private_token", "")
        
        if not private_token:
            return ActionResult(success=False, message="private_token is required", duration=time.time() - start_time)
        
        self._client = GitLabClient(url=url, private_token=private_token)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to GitLab at {url}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from GitLab."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(success=True, message="Disconnected from GitLab", duration=time.time() - start_time)
    
    def _list_projects(self, start_time: float) -> ActionResult:
        """List projects."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            projects = self._client.list_projects()
            return ActionResult(success=True, message=f"Found {len(projects)} projects", data={"projects": projects}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_project(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a project."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        if not project_id:
            return ActionResult(success=False, message="project_id is required", duration=time.time() - start_time)
        
        try:
            project = self._client.get_project(project_id)
            return ActionResult(success=project is not None, message=f"Found project: {project_id}" if project else f"Project not found: {project_id}", data={"project": project}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_branches(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List branches."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        if not project_id:
            return ActionResult(success=False, message="project_id is required", duration=time.time() - start_time)
        
        try:
            branches = self._client.list_branches(project_id)
            return ActionResult(success=True, message=f"Found {len(branches)} branches", data={"branches": branches}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_branch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a branch."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        branch = params.get("branch", "")
        ref = params.get("ref", "")
        
        if not project_id or not branch or not ref:
            return ActionResult(success=False, message="project_id, branch, and ref are required", duration=time.time() - start_time)
        
        try:
            result = self._client.create_branch(project_id, branch, ref)
            return ActionResult(success=result is not None, message=f"Branch created: {branch}" if result else "Create branch failed", data={"branch": result}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_pipelines(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List pipelines."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        if not project_id:
            return ActionResult(success=False, message="project_id is required", duration=time.time() - start_time)
        
        try:
            pipelines = self._client.list_pipelines(project_id, params.get("status"))
            return ActionResult(success=True, message=f"Found {len(pipelines)} pipelines", data={"pipelines": pipelines}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _trigger_pipeline(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Trigger a pipeline."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        ref = params.get("ref", "")
        
        if not project_id or not ref:
            return ActionResult(success=False, message="project_id and ref are required", duration=time.time() - start_time)
        
        try:
            pipeline = self._client.trigger_pipeline(project_id, ref, params.get("variables"))
            return ActionResult(success=pipeline is not None, message=f"Pipeline triggered: {pipeline.get('id')}" if pipeline else "Trigger pipeline failed", data={"pipeline": pipeline}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_pipeline(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a pipeline."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        pipeline_id = params.get("pipeline_id", 0)
        
        if not project_id or not pipeline_id:
            return ActionResult(success=False, message="project_id and pipeline_id are required", duration=time.time() - start_time)
        
        try:
            pipeline = self._client.get_pipeline(project_id, pipeline_id)
            return ActionResult(success=pipeline is not None, message=f"Pipeline #{pipeline_id} found", data={"pipeline": pipeline}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _retry_pipeline(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Retry a pipeline."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        pipeline_id = params.get("pipeline_id", 0)
        
        if not project_id or not pipeline_id:
            return ActionResult(success=False, message="project_id and pipeline_id are required", duration=time.time() - start_time)
        
        try:
            pipeline = self._client.retry_pipeline(project_id, pipeline_id)
            return ActionResult(success=pipeline is not None, message=f"Pipeline #{pipeline_id} retried", data={"pipeline": pipeline}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _cancel_pipeline(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Cancel a pipeline."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        pipeline_id = params.get("pipeline_id", 0)
        
        if not project_id or not pipeline_id:
            return ActionResult(success=False, message="project_id and pipeline_id are required", duration=time.time() - start_time)
        
        try:
            pipeline = self._client.cancel_pipeline(project_id, pipeline_id)
            return ActionResult(success=pipeline is not None, message=f"Pipeline #{pipeline_id} cancelled", data={"pipeline": pipeline}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_mrs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List merge requests."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        if not project_id:
            return ActionResult(success=False, message="project_id is required", duration=time.time() - start_time)
        
        try:
            mrs = self._client.list_merge_requests(project_id, params.get("state", "opened"))
            return ActionResult(success=True, message=f"Found {len(mrs)} merge requests", data={"merge_requests": mrs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_mr(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get a merge request."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        mr_iid = params.get("mr_iid", 0)
        
        if not project_id or not mr_iid:
            return ActionResult(success=False, message="project_id and mr_iid are required", duration=time.time() - start_time)
        
        try:
            mr = self._client.get_merge_request(project_id, mr_iid)
            return ActionResult(success=mr is not None, message=f"MR !{mr_iid} found", data={"merge_request": mr}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _merge_mr(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Merge a merge request."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        mr_iid = params.get("mr_iid", 0)
        
        if not project_id or not mr_iid:
            return ActionResult(success=False, message="project_id and mr_iid are required", duration=time.time() - start_time)
        
        try:
            mr = self._client.merge_merge_request(project_id, mr_iid, params.get("squash", False))
            return ActionResult(success=mr is not None, message=f"MR !{mr_iid} merged", data={"merge_request": mr}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _create_mr(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a merge request."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        title = params.get("title", "")
        source_branch = params.get("source_branch", "")
        target_branch = params.get("target_branch", "main")
        
        if not project_id or not title or not source_branch:
            return ActionResult(success=False, message="project_id, title, and source_branch are required", duration=time.time() - start_time)
        
        try:
            mr = self._client.create_merge_request(project_id, title, source_branch, target_branch, params.get("description", ""))
            return ActionResult(success=mr is not None, message=f"MR !{mr.get('iid')} created" if mr else "Create MR failed", data={"merge_request": mr}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _list_jobs(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List jobs for a pipeline."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        pipeline_id = params.get("pipeline_id", 0)
        
        if not project_id or not pipeline_id:
            return ActionResult(success=False, message="project_id and pipeline_id are required", duration=time.time() - start_time)
        
        try:
            jobs = self._client.list_jobs(project_id, pipeline_id)
            return ActionResult(success=True, message=f"Found {len(jobs)} jobs", data={"jobs": jobs}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _retry_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Retry a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        project_id = params.get("project_id", "")
        job_id = params.get("job_id", 0)
        
        if not project_id or not job_id:
            return ActionResult(success=False, message="project_id and job_id are required", duration=time.time() - start_time)
        
        try:
            job = self._client.retry_job(project_id, job_id)
            return ActionResult(success=job is not None, message=f"Job #{job_id} retried", data={"job": job}, duration=time.time() - start_time)
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
