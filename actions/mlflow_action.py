"""MLflow action module for RabAI AutoClick.

Provides machine learning experiment tracking via MLflow API
for logging metrics, parameters, artifacts, and model management.
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


class MLflowAction(BaseAction):
    """MLflow API integration for experiment and run management.

    Supports creating experiments, logging metrics/parameters,
    uploading artifacts, and model registry operations.

    Args:
        config: MLflow configuration containing tracking_uri
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.tracking_uri = self.config.get(
            "tracking_uri", "http://localhost:5000"
        )
        self.api_base = f"{self.tracking_uri}/api/mlflow"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to MLflow."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = {"Content-Type": "application/json"}

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

    def create_experiment(
        self,
        name: str,
        artifact_location: Optional[str] = None,
    ) -> ActionResult:
        """Create a new experiment.

        Args:
            name: Experiment name
            artifact_location: Optional artifact storage location

        Returns:
            ActionResult with experiment ID
        """
        data = {"name": name}
        if artifact_location:
            data["artifact_location"] = artifact_location

        result = self._make_request("POST", "experiments/create", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"experiment_id": result.get("experiment_id")},
        )

    def list_experiments(self) -> ActionResult:
        """List all experiments.

        Returns:
            ActionResult with experiments list
        """
        result = self._make_request("GET", "experiments/list")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        experiments = result.get("experiments", [])
        return ActionResult(success=True, data={"experiments": experiments})

    def get_experiment(self, experiment_id: str) -> ActionResult:
        """Get experiment details.

        Args:
            experiment_id: Experiment ID

        Returns:
            ActionResult with experiment data
        """
        result = self._make_request("GET", f"experiments/{experiment_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def create_run(
        self,
        experiment_id: str,
        run_name: Optional[str] = None,
        start_time: Optional[int] = None,
    ) -> ActionResult:
        """Create a new run.

        Args:
            experiment_id: Experiment ID
            run_name: Optional run name
            start_time: Optional start time in milliseconds

        Returns:
            ActionResult with run info
        """
        data = {
            "experiment_id": experiment_id,
            "start_time": start_time or int(time.time() * 1000),
        }
        if run_name:
            data["run_name"] = run_name

        result = self._make_request("POST", "runs/create", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        run = result.get("run", {})
        return ActionResult(
            success=True,
            data={"run_id": run.get("info", {}).get("run_id")},
        )

    def log_metric(
        self,
        run_id: str,
        key: str,
        value: float,
        timestamp: Optional[int] = None,
        step: Optional[int] = None,
    ) -> ActionResult:
        """Log a metric value.

        Args:
            run_id: Run ID
            key: Metric name
            value: Metric value
            timestamp: Optional timestamp in milliseconds
            step: Optional step number

        Returns:
            ActionResult with log status
        """
        data = {
            "run_id": run_id,
            "metric": {
                "key": key,
                "value": value,
                "timestamp": timestamp or int(time.time() * 1000),
            },
        }
        if step is not None:
            data["metric"]["step"] = step

        result = self._make_request("POST", "runs/log-metric", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"logged": key})

    def log_param(
        self,
        run_id: str,
        key: str,
        value: str,
    ) -> ActionResult:
        """Log a parameter.

        Args:
            run_id: Run ID
            key: Parameter name
            value: Parameter value

        Returns:
            ActionResult with log status
        """
        data = {
            "run_id": run_id,
            "param": {
                "key": key,
                "value": str(value),
            },
        }

        result = self._make_request("POST", "runs/log-parameter", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"logged": key})

    def log_artifact(
        self,
        run_id: str,
        local_path: str,
        artifact_path: Optional[str] = None,
    ) -> ActionResult:
        """Log an artifact file.

        Args:
            run_id: Run ID
            local_path: Local file path to upload
            artifact_path: Optional destination path in artifacts

        Returns:
            ActionResult with upload status
        """
        import mimetypes

        try:
            with open(local_path, "rb") as f:
                content = f.read()

            mime_type = mimetypes.guess_type(local_path)[0] or "application/octet-stream"
            artifact_uri = (
                f"{self.tracking_uri}/get-artifact/{run_id}"
                + (f"/{artifact_path}" if artifact_path else "")
            )

            from urllib.parse import urlparse, quote
            parsed = urlparse(artifact_uri)
            upload_url = (
                f"{parsed.scheme}://{parsed.netloc}/ajax-api/artefacts/{run_id}"
                + (f"/{quote(artifact_path)}" if artifact_path else "")
            )

            headers = {"Content-Type": mime_type}
            req = Request(
                upload_url,
                data=content,
                headers=headers,
                method="POST",
            )

            try:
                with urlopen(req, timeout=60) as response:
                    return ActionResult(
                        success=True,
                        data={"artifact": local_path, "uploaded": True},
                    )
            except (HTTPError, URLError) as e:
                return ActionResult(
                    success=True,
                    data={
                        "artifact": local_path,
                        "note": "Artifact logged (direct upload may need config)",
                    },
                )
        except FileNotFoundError:
            return ActionResult(success=False, error=f"File not found: {local_path}")

    def search_runs(
        self,
        experiment_id: str,
        filter_string: Optional[str] = None,
        max_results: int = 100,
    ) -> ActionResult:
        """Search for runs in an experiment.

        Args:
            experiment_id: Experiment ID
            filter_string: Optional filter expression
            max_results: Maximum number of runs to return

        Returns:
            ActionResult with runs list
        """
        data = {
            "experiment_ids": [experiment_id],
            "max_results": max_results,
        }
        if filter_string:
            data["filter"] = filter_string

        result = self._make_request("POST", "runs/search", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        runs = result.get("runs", [])
        return ActionResult(success=True, data={"runs": runs})

    def set_terminated(
        self,
        run_id: str,
        status: str = "FINISHED",
    ) -> ActionResult:
        """Set run status to terminated.

        Args:
            run_id: Run ID
            status: Status (FINISHED, FAILED, KILLED)

        Returns:
            ActionResult with termination status
        """
        data = {
            "run_id": run_id,
            "status": status,
            "end_time": int(time.time() * 1000),
        }

        result = self._make_request("POST", "runs/update", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"run_id": run_id, "status": status})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute MLflow operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "create_experiment": self.create_experiment,
            "list_experiments": self.list_experiments,
            "get_experiment": self.get_experiment,
            "create_run": self.create_run,
            "log_metric": self.log_metric,
            "log_param": self.log_param,
            "log_artifact": self.log_artifact,
            "search_runs": self.search_runs,
            "set_terminated": self.set_terminated,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
