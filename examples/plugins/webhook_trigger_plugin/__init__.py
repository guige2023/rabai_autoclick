"""
Webhook Trigger Plugin for RabAI AutoClick.

This plugin demonstrates custom trigger registration by providing
webhook-based triggering for workflows.
"""

from .webhook_trigger_plugin import WebhookTriggerPlugin, register

__all__ = ['WebhookTriggerPlugin', 'register']
__version__ = "1.0.0"
