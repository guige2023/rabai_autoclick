"""Make.com (Integromat) action module for RabAI AutoClick.

Provides automation via Make.com webhooks for scenario triggering
and data integration.
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


class MakeAction(BaseAction):
    """Make.com API integration for scenario automation.

    Triggers scenarios via webhooks, manages scenarios,
    and retrieves execution logs.

    Args:
        config: Make.com configuration containing api_key and team_id
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_key = self.config.get("api_key", "")
        self.team_id = self.config.get("team_id", "")
        self.api_base = "https://www.make.com/api/v2"
        self.headers = {
            "Authorization": f"Token {self.api_key}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Make.com API."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        req = Request(url, data=body, headers=self.headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def trigger_scenario_webhook(
        self,
        webhook_url: str,
        payload: Optional[Dict] = None,
    ) -> ActionResult:
        """Trigger a scenario via its webhook URL.

        Args:
            webhook_url: Full webhook URL from Make.com
            payload: Optional data payload to send

        Returns:
            ActionResult with trigger status
        """
        req = Request(
            webhook_url,
            data=json.dumps(payload or {}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(req, timeout=30) as response:
                result = response.read().decode("utf-8")
                return ActionResult(
                    success=True,
                    data={"response": result or "Webhook triggered successfully"},
                )
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return ActionResult(
                success=False,
                error=f"HTTP {e.code}: {error_body}",
            )
        except URLError as e:
            return ActionResult(success=False, error=f"URL error: {e.reason}")

    def list_scenarios(
        self,
        organization_id: Optional[str] = None,
    ) -> ActionResult:
        """List scenarios in an organization.

        Args:
            organization_id: Organization ID (uses team_id if not provided)

        Returns:
            ActionResult with scenarios list
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        org_id = organization_id or self.team_id
        if not org_id:
            return ActionResult(
                success=False,
                error="Missing organization_id or team_id",
            )

        result = self._make_request(
            "GET", f"organizations/{org_id}/scenarios"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True, data={"scenarios": result.get("scenarios", [])}
        )

    def get_scenario(
        self,
        scenario_id: str,
    ) -> ActionResult:
        """Get a scenario by ID.

        Args:
            scenario_id: Scenario ID

        Returns:
            ActionResult with scenario data
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request("GET", f"scenarios/{scenario_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def enable_scenario(self, scenario_id: str) -> ActionResult:
        """Enable a scenario.

        Args:
            scenario_id: Scenario ID

        Returns:
            ActionResult with status
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request(
            "PATCH",
            f"scenarios/{scenario_id}",
            data={"isActive": True},
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"enabled": True})

    def disable_scenario(self, scenario_id: str) -> ActionResult:
        """Disable a scenario.

        Args:
            scenario_id: Scenario ID

        Returns:
            ActionResult with status
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request(
            "PATCH",
            f"scenarios/{scenario_id}",
            data={"isActive": False},
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"enabled": False})

    def run_scenario(self, scenario_id: str) -> ActionResult:
        """Trigger immediate run of a scenario.

        Args:
            scenario_id: Scenario ID

        Returns:
            ActionResult with run status
        """
        if not self.api_key:
            return ActionResult(success=False, error="Missing api_key")

        result = self._make_request(
            "POST", f"scenarios/{scenario_id}/runs"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Make.com operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "trigger_webhook": self.trigger_scenario_webhook,
            "list_scenarios": self.list_scenarios,
            "get_scenario": self.get_scenario,
            "enable_scenario": self.enable_scenario,
            "disable_scenario": self.disable_scenario,
            "run_scenario": self.run_scenario,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
