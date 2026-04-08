"""Airflow DAG trigger action module for RabAI AutoClick.

Provides Apache Airflow DAG triggering via REST API.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AirflowTriggerAction(BaseAction):
    """Trigger Apache Airflow DAG runs via REST API."""
    action_type = "airflow_trigger"
    display_name = "Airflow触发"
    description = "Airflow DAG触发"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Trigger Airflow DAG.

        Args:
            context: Execution context.
            params: Dict with keys:
                - base_url: Airflow webserver URL
                - dag_id: DAG ID
                - conf: Optional DAG configuration dict
                - run_id: Optional unique run ID
                - logical_date: Optional logical date

        Returns:
            ActionResult with trigger response.
        """
        base_url = params.get('base_url', '')
        dag_id = params.get('dag_id', '')
        conf = params.get('conf', {})
        run_id = params.get('run_id', '')
        logical_date = params.get('logical_date', '')

        if not base_url:
            return ActionResult(success=False, message="base_url is required")
        if not dag_id:
            return ActionResult(success=False, message="dag_id is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            url = f"{base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
            payload: Dict[str, Any] = {}
            if conf:
                payload['conf'] = conf
            if run_id:
                payload['run_id'] = run_id
            if logical_date:
                payload['logical_date'] = logical_date
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            return ActionResult(
                success=True, message=f"Triggered DAG {dag_id}",
                data={
                    'dag_id': dag_id,
                    'run_id': data.get('run_id'),
                    'state': data.get('state'),
                },
                duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Airflow trigger error: {str(e)}")
