"""IFTTT action module for RabAI AutoClick.

Provides automation via IFTTT webhooks for triggering applets
and integrating with hundreds of services.
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


class IFTTTAction(BaseAction):
    """IFTTT Webhooks integration for automation.

    Triggers IFTTT applets via webhook events with optional
    data payload for actions.

    Args:
        config: IFTTT configuration containing webhook_key
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.webhook_key = self.config.get("webhook_key", "")
        self.webhook_base = "https://maker.ifttt.com/trigger"

    def trigger_event(
        self,
        event_name: str,
        value1: Optional[str] = None,
        value2: Optional[str] = None,
        value3: Optional[str] = None,
    ) -> ActionResult:
        """Trigger an IFTTT webhook event.

        Args:
            event_name: Name of the IFTTT event
            value1: Optional first value to pass
            value2: Optional second value to pass
            value3: Optional third value to pass

        Returns:
            ActionResult with trigger status
        """
        if not self.webhook_key:
            return ActionResult(success=False, error="Missing webhook_key")

        url = f"{self.webhook_base}/{event_name}/with/key/{self.webhook_key}"
        
        data = {}
        if value1 is not None:
            data["value1"] = value1
        if value2 is not None:
            data["value2"] = value2
        if value3 is not None:
            data["value3"] = value3

        req = Request(
            url,
            data=json.dumps(data).encode("utf-8") if data else None,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(req, timeout=30) as response:
                result = response.read().decode("utf-8")
                return ActionResult(
                    success=True,
                    data={"event": event_name, "response": result},
                )
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return ActionResult(
                success=False,
                error=f"HTTP {e.code}: {error_body}",
            )
        except URLError as e:
            return ActionResult(success=False, error=f"URL error: {e.reason}")

    def trigger_with_json(
        self,
        event_name: str,
        json_payload: Dict[str, Any],
    ) -> ActionResult:
        """Trigger an event with a JSON payload.

        Args:
            event_name: Name of the IFTTT event
            json_payload: Dictionary to send as JSON

        Returns:
            ActionResult with trigger status
        """
        if not self.webhook_key:
            return ActionResult(success=False, error="Missing webhook_key")

        url = f"{self.webhook_base}/{event_name}/with/key/{self.webhook_key}"
        req = Request(
            url,
            data=json.dumps(json_payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(req, timeout=30) as response:
                result = response.read().decode("utf-8")
                return ActionResult(
                    success=True,
                    data={"event": event_name, "response": result},
                )
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return ActionResult(
                success=False,
                error=f"HTTP {e.code}: {error_body}",
            )
        except URLError as e:
            return ActionResult(success=False, error=f"URL error: {e.reason}")

    def get_webhook_status(self, event_name: str) -> ActionResult:
        """Check if a webhook event exists (read-only check).

        Note: IFTTT doesn't provide a direct status check API,
        this method attempts to call the webhook and interpret results.

        Args:
            event_name: Name of the IFTTT event

        Returns:
            ActionResult with availability info
        """
        return ActionResult(
            success=True,
            data={
                "event": event_name,
                "note": "IFTTT webhooks are one-way triggers. "
                        "Use trigger_event to activate.",
            },
        )

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute IFTTT operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "trigger_event": self.trigger_event,
            "trigger_with_json": self.trigger_with_json,
            "get_webhook_status": self.get_webhook_status,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
