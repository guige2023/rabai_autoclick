"""Weights & Biases action module for RabAI AutoClick.

Provides experiment tracking and model management via W&B API
for ML experiment visualization and collaboration.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WandbAction(BaseAction):
    """Weights & Biases API integration for ML experiment tracking.

    Supports logging metrics, artifacts, runs, and project management.

    Args:
        config: W&B configuration containing api_key and entity
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_key = self.config.get("api_key", "")
        self.entity = self.config.get("entity", "")
        self.api_base = "https://api.wandb.ai/api/v1"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to W&B API."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def list_projects(self) -> ActionResult:
        """List all projects for the entity.

        Returns:
            ActionResult with projects list
        """
        if not self.api_key or not self.entity:
            return ActionResult(success=False, error="Missing api_key or entity")

        result = self._make_request(
            "GET", f"entities/{self.entity}/projects"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"projects": result.get("projects", [])},
        )

    def create_run(
        self,
        project: str,
        run_name: Optional[str] = None,
        config: Optional[Dict] = None,
    ) -> ActionResult:
        """Create a new run.

        Args:
            project: Project name
            run_name: Optional run name
            config: Optional run configuration

        Returns:
            ActionResult with run info
        """
        if not self.api_key or not self.entity:
            return ActionResult(success=False, error="Missing api_key or entity")

        data = {"displayName": run_name or f"run-{int(time.time())}"}
        if config:
            data["config"] = config

        result = self._make_request(
            "POST",
            f"entities/{self.entity}/projects/{project}/runs",
            data=data,
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={
                "name": result.get("name"),
                "display_name": result.get("displayName"),
                "id": result.get("id"),
            },
        )

    def log_metrics(
        self,
        project: str,
        run_name: str,
        metrics: Dict[str, float],
        step: Optional[int] = None,
    ) -> ActionResult:
        """Log metrics to a run.

        Args:
            project: Project name
            run_name: Run name
            metrics: Dict of metric name -> value
            step: Optional step number

        Returns:
            ActionResult with log status
        """
        if not self.api_key or not self.entity:
            return ActionResult(success=False, error="Missing api_key or entity")

        data = {
            "metrics": [
                {"key": k, "value": v, "step": step or 0}
                for k, v in metrics.items()
            ]
        }

        result = self._make_request(
            "POST",
            f"entities/{self.entity}/projects/{project}/runs/{run_name}/metrics",
            data=data,
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"logged": len(metrics)})

    def upload_artifact(
        self,
        project: str,
        run_name: str,
        artifact_name: str,
        artifact_type: str = "model",
        local_path: Optional[str] = None,
    ) -> ActionResult:
        """Upload an artifact.

        Args:
            project: Project name
            run_name: Run name
            artifact_name: Artifact name
            artifact_type: Artifact type (model, dataset, etc.)
            local_path: Optional local file path

        Returns:
            ActionResult with upload status
        """
        if not self.api_key or not self.entity:
            return ActionResult(success=False, error="Missing api_key or entity")

        data = {
            "name": artifact_name,
            "type": artifact_type,
        }
        if local_path:
            try:
                with open(local_path, "rb") as f:
                    import base64
                    content = base64.b64encode(f.read()).decode()
                    data["artifact"] = content
            except FileNotFoundError:
                return ActionResult(success=False, error=f"File not found: {local_path}")

        result = self._make_request(
            "POST",
            f"entities/{self.entity}/projects/{project}/runs/{run_name}/artifacts",
            data=data,
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"artifact_name": artifact_name, "uploaded": True},
        )

    def list_runs(
        self,
        project: str,
        limit: int = 100,
    ) -> ActionResult:
        """List runs in a project.

        Args:
            project: Project name
            limit: Maximum runs to return

        Returns:
            ActionResult with runs list
        """
        if not self.api_key or not self.entity:
            return ActionResult(success=False, error="Missing api_key or entity")

        result = self._make_request(
            "GET",
            f"entities/{self.entity}/projects/{project}/runs?limit={limit}",
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"runs": result.get("runs", [])},
        )

    def get_run(self, project: str, run_name: str) -> ActionResult:
        """Get run details.

        Args:
            project: Project name
            run_name: Run name

        Returns:
            ActionResult with run data
        """
        if not self.api_key or not self.entity:
            return ActionResult(success=False, error="Missing api_key or entity")

        result = self._make_request(
            "GET", f"entities/{self.entity}/projects/{project}/runs/{run_name}"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute W&B operation."""
        operations = {
            "list_projects": self.list_projects,
            "create_run": self.create_run,
            "log_metrics": self.log_metrics,
            "upload_artifact": self.upload_artifact,
            "list_runs": self.list_runs,
            "get_run": self.get_run,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
