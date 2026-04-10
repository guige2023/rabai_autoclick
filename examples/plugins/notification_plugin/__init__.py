"""
Notification Plugin for RabAI AutoClick.

This plugin demonstrates custom action registration by providing
a send_notification action that can be used in workflows.
"""

from .notification_plugin import NotificationPlugin, register

__all__ = ['NotificationPlugin', 'register']
__version__ = "1.0.0"
