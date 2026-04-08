"""PagerDuty action module for RabAI AutoClick.

Provides integration with PagerDuty API for incident management,
on-call schedules, and alert automation.
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


class PagerDutyAction(BaseAction):
    """PagerDuty API integration for incident and on-call management.

    Supports incident creation, acknowledgment, escalation,
    and schedule management.

    Args:
        config: PagerDuty configuration containing routing_key (events API)
                or api_key (rest API)
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.routing_key = self.config.get("routing_key", "")
        self.api_key = self.config.get("api_key", "")
        self.events_api = "https://events.pagerduty.com/v2"
        self.rest_api = "https://api.pagerduty.com"

    def _rest_headers(self) -> Dict[str, str]:
        """Get headers for REST API requests."""
        return {
            "Authorization": f"Token token={self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/vnd.pagerduty+json;version=2",
        }

    def _make_rest_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to PagerDuty REST API."""
        url = f"{self.rest_api}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = self._rest_headers()
        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result.get("data", result)
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def send_event(
        self,
        action: str,
        dedup_key: Optional[str] = None,
        severity: str = "info",
        source: str = "automated",
        summary: str = "",
        custom_details: Optional[Dict] = None,
    ) -> ActionResult:
        """Send an event to PagerDuty Events API.

        Args:
            action: Event action (trigger, acknowledge, resolve)
            dedup_key: Deduplication key for correlation
            severity: Severity level (critical, error, warning, info)
            source: Event source identifier
            summary: Brief summary of the event
            custom_details: Additional event details

        Returns:
            ActionResult with event response
        """
        if not self.routing_key:
            return ActionResult(success=False, error="Missing routing_key")

        payload = {
            "routing_key": self.routing_key,
            "event_action": action,
            "dedup_key": dedup_key,
            "payload": {
                "severity": severity,
                "source": source,
                "summary": summary,
                "custom_details": custom_details or {},
            },
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        req = Request(
            f"{self.events_api}/enqueue",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return ActionResult(success=True, data=result)
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return ActionResult(success=False, error=f"HTTP {e.code}: {error_body}")
        except URLError as e:
            return ActionResult(success=False, error=f"URL error: {e.reason}")

    def trigger_incident(
        self,
        title: str,
        severity: str = "warning",
        source: str = "automated",
        dedup_key: Optional[str] = None,
        custom_details: Optional[Dict] = None,
    ) -> ActionResult:
        """Trigger a new incident.

        Args:
            title: Incident title
            severity: Severity level
            source: Source of the incident
            dedup_key: Deduplication key
            custom_details: Additional details

        Returns:
            ActionResult with incident response
        """
        return self.send_event(
            action="trigger",
            dedup_key=dedup_key,
            severity=severity,
            source=source,
            summary=title,
            custom_details=custom_details,
        )

    def acknowledge_incident(self, dedup_key: str) -> ActionResult:
        """Acknowledge an incident.

        Args:
            dedup_key: Deduplication key of the incident

        Returns:
            ActionResult with acknowledgment response
        """
        return self.send_event(action="acknowledge", dedup_key=dedup_key)

    def resolve_incident(self, dedup_key: str) -> ActionResult:
        """Resolve an incident.

        Args:
            dedup_key: Deduplication key of the incident

        Returns:
            ActionResult with resolution response
        """
        return self.send_event(action="resolve", dedup_key=dedup_key)

    def list_incidents(
        self,
        statuses: Optional[List[str]] = None,
        limit: int = 100,
    ) -> ActionResult:
        """List incidents from PagerDuty REST API.

        Args:
            statuses: Filter by statuses (triggered, acknowledged, resolved)
            limit: Maximum number of incidents to return

        Returns:
            ActionResult with incidents list
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key for REST API")

        params = {"limit": limit}
        if statuses:
            params["statuses[]"] = statuses

        result = self._make_rest_request("GET", "incidents", params=params)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"incidents": result.get("incidents", [])})

    def get_on_call(self, schedule_id: Optional[str] = None) -> ActionResult:
        """Get current on-call users.

        Args:
            schedule_id: Optional schedule ID to filter by

        Returns:
            ActionResult with on-call information
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        endpoint = "oncalls"
        if schedule_id:
            endpoint = f"schedules/{schedule_id}/oncalls"

        result = self._make_rest_request("GET", endpoint)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"oncalls": result.get("oncalls", [])})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute PagerDuty operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "trigger_incident": self.trigger_incident,
            "acknowledge_incident": self.acknowledge_incident,
            "resolve_incident": self.resolve_incident,
            "list_incidents": self.list_incidents,
            "get_on_call": self.get_on_call,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
