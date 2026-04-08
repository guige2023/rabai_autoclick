"""Airflow action module for RabAI AutoClick.

Provides workflow orchestration via Apache Airflow REST API
for DAG management, task execution, and monitoring.
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


class AirflowAction(BaseAction):
    """Airflow REST API integration for workflow orchestration.

    Supports DAG triggering, task status monitoring,
    variable/connection management.

    Args:
        config: Airflow configuration containing host, username, password
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.host = self.config.get("host", "http://localhost:8080")
        self.username = self.config.get("username", "airflow")
        self.password = self.config.get("password", "airflow")
        self.api_base = f"{self.host}/api/v1"

        import base64
        creds = f"{self.username}:{self.password}"
        token = base64.b64encode(creds.encode()).decode()
        self.headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Airflow."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = dict(self.headers)
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

    def trigger_dag(
        self,
        dag_id: str,
        conf: Optional[Dict] = None,
        execution_date: Optional[str] = None,
    ) -> ActionResult:
        """Trigger a DAG run.

        Args:
            dag_id: DAG identifier
            conf: Optional configuration dict
            execution_date: Optional execution date

        Returns:
            ActionResult with DAG run info
        """
        data = {}
        if conf:
            data["conf"] = conf
        if execution_date:
            data["execution_date"] = execution_date

        result = self._make_request(
            "POST", f"dags/{dag_id}/dagRuns", data=data
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={
                "dag_run_id": result.get("dag_run_id"),
                "state": result.get("state"),
            },
        )

    def list_dag_runs(
        self,
        dag_id: str,
        limit: int = 100,
    ) -> ActionResult:
        """List DAG runs.

        Args:
            dag_id: DAG identifier
            limit: Maximum runs to return

        Returns:
            ActionResult with runs list
        """
        result = self._make_request(
            "GET", f"dags/{dag_id}/dagRuns?limit={limit}"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"dag_runs": result.get("dag_runs", [])},
        )

    def get_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
    ) -> ActionResult:
        """Get a DAG run.

        Args:
            dag_id: DAG identifier
            dag_run_id: Run identifier

        Returns:
            ActionResult with run data
        """
        result = self._make_request("GET", f"dags/{dag_id}/dagRuns/{dag_run_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def delete_dag_run(
        self,
        dag_id: str,
        dag_run_id: str,
    ) -> ActionResult:
        """Delete a DAG run.

        Args:
            dag_id: DAG identifier
            dag_run_id: Run identifier

        Returns:
            ActionResult with deletion status
        """
        result = self._make_request(
            "DELETE", f"dags/{dag_id}/dagRuns/{dag_run_id}"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"deleted": True})

    def list_dags(self) -> ActionResult:
        """List all DAGs.

        Returns:
            ActionResult with DAGs list
        """
        result = self._make_request("GET", "dags")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        dags = result.get("dags", [])
        return ActionResult(success=True, data={"dags": dags})

    def get_task_instances(
        self,
        dag_id: str,
        dag_run_id: str,
    ) -> ActionResult:
        """Get task instances for a DAG run.

        Args:
            dag_id: DAG identifier
            dag_run_id: Run identifier

        Returns:
            ActionResult with task instances
        """
        result = self._make_request(
            "GET", f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"task_instances": result.get("task_instances", [])},
        )

    def get_variable(self, key: str) -> ActionResult:
        """Get an Airflow variable.

        Args:
            key: Variable key

        Returns:
            ActionResult with variable value
        """
        result = self._make_request("GET", f"variables/{key}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"key": key, "value": result.get("value")},
        )

    def set_variable(
        self,
        key: str,
        value: str,
    ) -> ActionResult:
        """Set an Airflow variable.

        Args:
            key: Variable key
            value: Variable value

        Returns:
            ActionResult with set status
        """
        result = self._make_request(
            "POST", "variables", data={"key": key, "value": value}
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"key": key, "set": True})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Airflow operation."""
        operations = {
            "trigger_dag": self.trigger_dag,
            "list_dag_runs": self.list_dag_runs,
            "get_dag_run": self.get_dag_run,
            "delete_dag_run": self.delete_dag_run,
            "list_dags": self.list_dags,
            "get_task_instances": self.get_task_instances,
            "get_variable": self.get_variable,
            "set_variable": self.set_variable,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
