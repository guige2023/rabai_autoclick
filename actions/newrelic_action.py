"""New Relic action module for RabAI AutoClick.

Provides observability operations via New Relic API for
metrics, events, alerts, and infrastructure monitoring.
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


class NewRelicAction(BaseAction):
    """New Relic API integration for observability operations.

    Supports metric data ingestion, event posting, alert policy
    management, and dashboard operations.

    Args:
        config: New Relic configuration containing api_key and account_id
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_key = self.config.get("api_key", "")
        self.account_id = self.config.get("account_id", "")
        self.eu_region = self.config.get("eu_region", False)
        self.api_base = (
            "https://metric-api.eu.newrelic.com" if self.eu_region
            else "https://metric-api.newrelic.com"
        )
        self.event_api = (
            "https://insights.eu.newrelic.com" if self.eu_region
            else "https://insights-api.newrelic.com"
        )

    def _make_request(
        self,
        method: str,
        url: str,
        data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to New Relic."""
        body = json.dumps(data).encode("utf-8") if data else None
        req_headers = {
            "Content-Type": "application/json",
            "Api-Key": self.api_key,
        }
        if headers:
            req_headers.update(headers)

        req = Request(url, data=body, headers=req_headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = response.read().decode("utf-8")
                if result:
                    return json.loads(result)
                return {"success": True}
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def post_metric(
        self,
        metrics: List[Dict[str, Any]],
    ) -> ActionResult:
        """Post metric data points.

        Args:
            metrics: List of metric payloads with name, value, timestamp, attributes

        Returns:
            ActionResult with post status
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        payload = [
            {
                "metricType": m.get("type", "gauge"),
                "metricName": m["name"],
                "timestamp": m.get("timestamp", int(time.time())),
                "value": m["value"],
                "attributes": m.get("attributes", {}),
            }
            for m in metrics
        ]

        result = self._make_request(
            "POST",
            f"{self.api_base}/metric/v1",
            data=payload,
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"metrics_accepted": len(metrics)})

    def post_event(
        self,
        event_type: str,
        attributes: Dict[str, Any],
    ) -> ActionResult:
        """Post a custom event.

        Args:
            event_type: Name of the event type
            attributes: Event attributes

        Returns:
            ActionResult with post status
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        payload = [
            {
                "eventType": event_type,
                **attributes,
            }
        ]

        result = self._make_request(
            "POST",
            f"{self.event_api}/pubapi/v1/accounts/{self.account_id}/events",
            data=payload,
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"events_accepted": 1})

    def query_events(
        self,
        event_type: str,
        limit: int = 100,
    ) -> ActionResult:
        """Query events using NRQL.

        Args:
            event_type: Event type to query
            limit: Maximum events to return

        Returns:
            ActionResult with events
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        nrql = f"SELECT * FROM {event_type} LIMIT {limit}"
        result = self._make_request(
            "GET",
            f"{self.event_api}/pubapi/v1/accounts/{self.account_id}/query",
            headers={"NRQL": nrql},
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"events": result.get("events", [])})

    def list_alert_policies(self) -> ActionResult:
        """List alert policies.

        Returns:
            ActionResult with policies list
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request(
            "GET",
            f"https://api.newrelic.com/v2/alerts_policies.json",
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        policies = result.get("policies", [])
        return ActionResult(success=True, data={"policies": policies})

    def create_alert_policy(
        self,
        name: str,
        incident_preference: str = "PER_POLICY",
    ) -> ActionResult:
        """Create an alert policy.

        Args:
            name: Policy name
            incident_preference: PER_POLICY, PER_CONDITION, or PER_CONDITION_AND_TARGET

        Returns:
            ActionResult with created policy
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        data = {
            "policy": {
                "name": name,
                "incident_preference": incident_preference,
            }
        }

        result = self._make_request(
            "POST",
            f"https://api.newrelic.com/v2/alerts_policies.json",
            data=data,
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        policy = result.get("policy", {})
        return ActionResult(success=True, data={"policy_id": policy.get("id")})

    def list_dashboards(self) -> ActionResult:
        """List dashboards.

        Returns:
            ActionResult with dashboards list
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request(
            "GET",
            f"https://api.newrelic.com/v2/dashboards.json",
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        dashboards = result.get("dashboards", [])
        return ActionResult(success=True, data={"dashboards": dashboards})

    def get_dashboard(self, dashboard_id: str) -> ActionResult:
        """Get a dashboard by ID.

        Args:
            dashboard_id: Dashboard ID

        Returns:
            ActionResult with dashboard data
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request(
            "GET",
            f"https://api.newrelic.com/v2/dashboards/{dashboard_id}.json",
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result.get("dashboard", {}))

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute New Relic operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "post_metric": self.post_metric,
            "post_event": self.post_event,
            "query_events": self.query_events,
            "list_alert_policies": self.list_alert_policies,
            "create_alert_policy": self.create_alert_policy,
            "list_dashboards": self.list_dashboards,
            "get_dashboard": self.get_dashboard,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
