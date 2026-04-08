"""Jenkins action module for RabAI AutoClick.

Provides Jenkins CI/CD operations including
job management, build triggering, and pipeline control.
"""

import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class BuildInfo:
    """Represents a Jenkins build.
    
    Attributes:
        number: Build number.
        result: Build result (SUCCESS, FAILURE, etc.).
        duration: Build duration in milliseconds.
        timestamp: Build timestamp.
        url: Build URL.
    """
    number: int
    result: Optional[str]
    duration: int
    timestamp: int
    url: str


class JenkinsClient:
    """Jenkins client for CI/CD operations.
    
    Provides methods for managing Jenkins jobs,
    triggering builds, and monitoring pipeline status.
    """
    
    def __init__(
        self,
        url: str = "http://localhost:8080",
        username: str = "",
        api_token: str = ""
    ) -> None:
        """Initialize Jenkins client.
        
        Args:
            url: Jenkins server URL.
            username: Username for authentication.
            api_token: API token for authentication.
        """
        self.url = url.rstrip("/")
        self.username = username
        self.api_token = api_token
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to Jenkins server.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required. Install with: pip install requests")
        
        try:
            self._session = requests.Session()
            if self.username and self.api_token:
                self._session.auth = (self.username, self.api_token)
            
            response = self._session.get(
                f"{self.url}/api/json",
                timeout=30
            )
            
            return response.status_code == 200
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the Jenkins session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def list_jobs(self, depth: int = 1) -> List[Dict[str, Any]]:
        """List all jobs.
        
        Args:
            depth: JSON tree depth.
            
        Returns:
            List of job information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            response = self._session.get(
                f"{self.url}/api/json",
                params={"tree": f"jobs[name,url,color]{',' * (depth > 1)}"},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("jobs", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"List jobs failed: {str(e)}")
    
    def get_job_info(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a job.
        
        Args:
            job_name: Job name.
            
        Returns:
            Job information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.get(
                f"{self.url}/job/{encoded_name}/api/json",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def get_build_info(self, job_name: str, build_number: int) -> Optional[Dict[str, Any]]:
        """Get information about a build.
        
        Args:
            job_name: Job name.
            build_number: Build number.
            
        Returns:
            Build information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.get(
                f"{self.url}/job/{encoded_name}/{build_number}/api/json",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def trigger_build(
        self,
        job_name: str,
        parameters: Optional[Dict[str, str]] = None,
        token: Optional[str] = None
    ) -> bool:
        """Trigger a build.
        
        Args:
            job_name: Job name.
            parameters: Optional build parameters.
            token: Optional build token.
            
        Returns:
            True if build triggered successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            if parameters:
                response = self._session.post(
                    f"{self.url}/job/{encoded_name}/buildWithParameters",
                    data=parameters,
                    timeout=60
                )
            elif token:
                response = self._session.post(
                    f"{self.url}/job/{encoded_name}/build",
                    params={"token": token},
                    timeout=60
                )
            else:
                response = self._session.post(
                    f"{self.url}/job/{encoded_name}/build",
                    timeout=60
                )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def get_build_console_output(self, job_name: str, build_number: int) -> str:
        """Get console output for a build.
        
        Args:
            job_name: Job name.
            build_number: Build number.
            
        Returns:
            Console output text.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.get(
                f"{self.url}/job/{encoded_name}/{build_number}/consoleText",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.text
            
            return ""
        
        except Exception:
            return ""
    
    def stop_build(self, job_name: str, build_number: int) -> bool:
        """Stop a running build.
        
        Args:
            job_name: Job name.
            build_number: Build number.
            
        Returns:
            True if build stopped successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.post(
                f"{self.url}/job/{encoded_name}/{build_number}/stop",
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def get_last_build_info(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Get information about the last build.
        
        Args:
            job_name: Job name.
            
        Returns:
            Last build information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.get(
                f"{self.url}/job/{encoded_name}/lastBuild/api/json",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def get_last_stable_build(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Get the last stable build.
        
        Args:
            job_name: Job name.
            
        Returns:
            Last stable build information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.get(
                f"{self.url}/job/{encoded_name}/lastStableBuild/api/json",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def get_last_successful_build(self, job_name: str) -> Optional[Dict[str, Any]]:
        """Get the last successful build.
        
        Args:
            job_name: Job name.
            
        Returns:
            Last successful build information or None.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.get(
                f"{self.url}/job/{encoded_name}/lastSuccessfulBuild/api/json",
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
        
        except Exception:
            return None
    
    def enable_job(self, job_name: str) -> bool:
        """Enable a job.
        
        Args:
            job_name: Job name.
            
        Returns:
            True if enabled successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.post(
                f"{self.url}/job/{encoded_name}/enable",
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def disable_job(self, job_name: str) -> bool:
        """Disable a job.
        
        Args:
            job_name: Job name.
            
        Returns:
            True if disabled successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.post(
                f"{self.url}/job/{encoded_name}/disable",
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def delete_job(self, job_name: str) -> bool:
        """Delete a job.
        
        Args:
            job_name: Job name.
            
        Returns:
            True if deleted successfully.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            import urllib.parse
            
            encoded_name = urllib.parse.quote(job_name, safe="")
            
            response = self._session.post(
                f"{self.url}/job/{encoded_name}/doDelete",
                timeout=30
            )
            
            return response.status_code in (200, 201)
        
        except Exception:
            return False
    
    def get_queue_info(self) -> List[Dict[str, Any]]:
        """Get the build queue information.
        
        Returns:
            List of queued items.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            response = self._session.get(
                f"{self.url}/queue/api/json",
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("items", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"Get queue info failed: {str(e)}")
    
    def get_computer_info(self) -> List[Dict[str, Any]]:
        """Get all computer (agent) information.
        
        Returns:
            List of computer information.
        """
        if not self._session:
            raise RuntimeError("Not connected to Jenkins")
        
        try:
            response = self._session.get(
                f"{self.url}/computer/api/json",
                params={"tree": "computer[displayName,executables[number],idle,temporarilyOffline]"},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("computer", [])
            
            return []
        
        except Exception as e:
            raise Exception(f"Get computer info failed: {str(e)}")


class JenkinsAction(BaseAction):
    """Jenkins action for CI/CD operations.
    
    Supports job management, build triggering, and pipeline control.
    """
    action_type: str = "jenkins"
    display_name: str = "Jenkins动作"
    description: str = "Jenkins CI/CD作业管理和构建触发"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[JenkinsClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Jenkins operation.
        
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
            elif operation == "list_jobs":
                return self._list_jobs(start_time)
            elif operation == "get_job_info":
                return self._get_job_info(params, start_time)
            elif operation == "get_build_info":
                return self._get_build_info(params, start_time)
            elif operation == "trigger_build":
                return self._trigger_build(params, start_time)
            elif operation == "get_console_output":
                return self._get_console_output(params, start_time)
            elif operation == "stop_build":
                return self._stop_build(params, start_time)
            elif operation == "get_last_build":
                return self._get_last_build(params, start_time)
            elif operation == "enable_job":
                return self._enable_job(params, start_time)
            elif operation == "disable_job":
                return self._disable_job(params, start_time)
            elif operation == "delete_job":
                return self._delete_job(params, start_time)
            elif operation == "get_queue":
                return self._get_queue(start_time)
            elif operation == "get_computers":
                return self._get_computers(start_time)
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
                message=f"Jenkins operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to Jenkins."""
        url = params.get("url", "http://localhost:8080")
        username = params.get("username", "")
        api_token = params.get("api_token", "")
        
        self._client = JenkinsClient(url=url, username=username, api_token=api_token)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to Jenkins at {url}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from Jenkins."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from Jenkins",
            duration=time.time() - start_time
        )
    
    def _list_jobs(self, start_time: float) -> ActionResult:
        """List all jobs."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            jobs = self._client.list_jobs()
            return ActionResult(
                success=True,
                message=f"Found {len(jobs)} jobs",
                data={"jobs": jobs, "count": len(jobs)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_job_info(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get information about a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_name = params.get("job_name", "")
        if not job_name:
            return ActionResult(success=False, message="job_name is required", duration=time.time() - start_time)
        
        try:
            info = self._client.get_job_info(job_name)
            return ActionResult(
                success=info is not None,
                message=f"Found job: {job_name}" if info else f"Job not found: {job_name}",
                data={"job": info},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_build_info(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get information about a build."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_name = params.get("job_name", "")
        build_number = params.get("build_number", 0)
        
        if not job_name or not build_number:
            return ActionResult(success=False, message="job_name and build_number are required", duration=time.time() - start_time)
        
        try:
            info = self._client.get_build_info(job_name, build_number)
            return ActionResult(
                success=info is not None,
                message=f"Build #{build_number} info retrieved",
                data={"build": info},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _trigger_build(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Trigger a build."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_name = params.get("job_name", "")
        if not job_name:
            return ActionResult(success=False, message="job_name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.trigger_build(
                job_name=job_name,
                parameters=params.get("parameters"),
                token=params.get("token")
            )
            return ActionResult(
                success=success,
                message=f"Build triggered: {job_name}" if success else "Trigger build failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_console_output(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get console output for a build."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_name = params.get("job_name", "")
        build_number = params.get("build_number", 0)
        
        if not job_name or not build_number:
            return ActionResult(success=False, message="job_name and build_number are required", duration=time.time() - start_time)
        
        try:
            output = self._client.get_build_console_output(job_name, build_number)
            return ActionResult(
                success=bool(output),
                message=f"Console output retrieved ({len(output)} chars)",
                data={"output": output},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _stop_build(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Stop a running build."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_name = params.get("job_name", "")
        build_number = params.get("build_number", 0)
        
        if not job_name or not build_number:
            return ActionResult(success=False, message="job_name and build_number are required", duration=time.time() - start_time)
        
        try:
            success = self._client.stop_build(job_name, build_number)
            return ActionResult(
                success=success,
                message=f"Build #{build_number} stopped" if success else "Stop build failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_last_build(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get the last build info."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_name = params.get("job_name", "")
        if not job_name:
            return ActionResult(success=False, message="job_name is required", duration=time.time() - start_time)
        
        build_type = params.get("type", "last")  # last, stable, successful
        
        try:
            if build_type == "stable":
                info = self._client.get_last_stable_build(job_name)
            elif build_type == "successful":
                info = self._client.get_last_successful_build(job_name)
            else:
                info = self._client.get_last_build_info(job_name)
            
            return ActionResult(
                success=info is not None,
                message=f"Last {build_type} build info retrieved",
                data={"build": info},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _enable_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Enable a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_name = params.get("job_name", "")
        if not job_name:
            return ActionResult(success=False, message="job_name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.enable_job(job_name)
            return ActionResult(
                success=success,
                message=f"Job enabled: {job_name}" if success else "Enable job failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _disable_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Disable a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_name = params.get("job_name", "")
        if not job_name:
            return ActionResult(success=False, message="job_name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.disable_job(job_name)
            return ActionResult(
                success=success,
                message=f"Job disabled: {job_name}" if success else "Disable job failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _delete_job(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a job."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        job_name = params.get("job_name", "")
        if not job_name:
            return ActionResult(success=False, message="job_name is required", duration=time.time() - start_time)
        
        try:
            success = self._client.delete_job(job_name)
            return ActionResult(
                success=success,
                message=f"Job deleted: {job_name}" if success else "Delete job failed",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_queue(self, start_time: float) -> ActionResult:
        """Get build queue information."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            queue = self._client.get_queue_info()
            return ActionResult(
                success=True,
                message=f"Found {len(queue)} items in queue",
                data={"queue": queue, "count": len(queue)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _get_computers(self, start_time: float) -> ActionResult:
        """Get computer (agent) information."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            computers = self._client.get_computer_info()
            return ActionResult(
                success=True,
                message=f"Found {len(computers)} computers",
                data={"computers": computers, "count": len(computers)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
