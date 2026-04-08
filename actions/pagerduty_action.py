"""PagerDuty action module for RabAI AutoClick.

Provides PagerDuty API operations for incident management and alerts.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PagerDutyTriggerAction(BaseAction):
    """Trigger PagerDuty incidents and send alerts."""
    action_type = "pagerduty_trigger"
    display_name = "PagerDuty告警"
    description = "PagerDuty告警触发"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Trigger PagerDuty incident.

        Args:
            context: Execution context.
            params: Dict with keys:
                - routing_key: PagerDuty integration key
                - summary: Incident summary
                - severity: 'critical', 'error', 'warning', 'info'
                - source: Source of the incident
                - custom_details: Optional additional details

        Returns:
            ActionResult with incident data.
        """
        routing_key = params.get('routing_key', '') or os.environ.get('PAGERDUTY_ROUTING_KEY')
        summary = params.get('summary', '')
        severity = params.get('severity', 'warning')
        source = params.get('source', 'RabAI AutoClick')
        custom_details = params.get('custom_details', {})

        if not routing_key:
            return ActionResult(success=False, message="routing_key is required")
        if not summary:
            return ActionResult(success=False, message="summary is required")

        try:
            import requests
        except ImportError:
            return ActionResult(success=False, message="requests not installed")

        start = time.time()
        try:
            response = requests.post(
                'https://events.pagerduty.com/v2/enqueue',
                json={
                    'routing_key': routing_key,
                    'event_action': 'trigger',
                    'payload': {
                        'summary': summary,
                        'severity': severity,
                        'source': source,
                        'custom_details': custom_details,
                    }
                },
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            duration = time.time() - start
            return ActionResult(
                success=True, message="PagerDuty incident triggered",
                data={'incident_key': data.get('incident_key'), 'dedup_key': data.get('dedup_key')}, duration=duration
            )
        except Exception as e:
            return ActionResult(success=False, message=f"PagerDuty error: {str(e)}")
