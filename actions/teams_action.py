"""Microsoft Teams integration for RabAI AutoClick.

Provides actions to send messages, manage channels, and interact with Teams.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TeamsMessageAction(BaseAction):
    """Send messages to Microsoft Teams channels and chats.

    Supports adaptive cards, simple text, and webhooks.
    """
    action_type = "teams_message"
    display_name = "Teams消息"
    description = "向Microsoft Teams发送消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Send a Teams message.

        Args:
            context: Execution context.
            params: Dict with keys:
                - webhook_url: Teams incoming webhook URL
                - message: Message text
                - title: Optional card title
                - color: Optional color hex code
                - adaptive_card: Adaptive card JSON dict
                - mentions: List of @mention dicts with id and mention_text

        Returns:
            ActionResult with send result.
        """
        webhook_url = params.get('webhook_url') or os.environ.get('TEAMS_WEBHOOK_URL')
        message = params.get('message', '')
        adaptive_card = params.get('adaptive_card')

        if not webhook_url:
            return ActionResult(success=False, message="webhook_url or TEAMS_WEBHOOK_URL is required")

        import urllib.request
        import urllib.error

        try:
            if adaptive_card:
                payload = adaptive_card
            elif params.get('title') or params.get('color'):
                # Office 365 connector card format
                payload = {
                    '@type': 'MessageCard',
                    '@context': 'http://schema.org/extensions',
                    'themeColor': params.get('color', '0078D4'),
                    'summary': params.get('title', 'Teams Message'),
                    'sections': [{
                        'activityTitle': params.get('title', ''),
                        'activitySubtitle': params.get('subtitle', ''),
                        'text': message,
                        'facts': [
                            {'name': k, 'value': v}
                            for k, v in params.get('facts', {}).items()
                        ],
                    }],
                }
                if params.get('potentialAction'):
                    payload['potentialAction'] = params['potentialAction']
            else:
                # Simple text message
                payload = {'text': message}

            if params.get('mentions'):
                mentions_section = {'mentions': params['mentions']}
                if isinstance(payload, dict) and 'sections' in payload:
                    if len(payload['sections']) > 0:
                        payload['sections'][0]['mentions'] = params['mentions']

            req = urllib.request.Request(
                webhook_url,
                data=json.dumps(payload).encode('utf-8'),
                method='POST',
                headers={'Content-Type': 'application/json'}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode('utf-8') if resp.status != 200 else ''

            if resp.status not in (200, 201, 204):
                return ActionResult(success=False, message=f"Teams error: {resp.status}", data={'body': body})

            return ActionResult(success=True, message="Teams message sent", data={'status': resp.status})
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Teams API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Teams error: {str(e)}")


class TeamsChannelAction(BaseAction):
    """Manage Microsoft Teams channels.

    Supports listing channels, sending messages, and managing channel membership.
    """
    action_type = "teams_channel"
    display_name = "Teams频道"
    description = "管理Microsoft Teams频道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Teams channels.

        Args:
            context: Execution context.
            params: Dict with keys:
                - tenant_id: Azure AD tenant ID
                - client_id: Azure app client ID
                - client_secret: Azure app client secret
                - team_id: Team ID
                - channel_id: Channel ID
                - operation: list_channels | send_message | get_message | list_messages
                - message: Message text (for send_message)

        Returns:
            ActionResult with channel data.
        """
        tenant_id = params.get('tenant_id') or os.environ.get('TEAMS_TENANT_ID')
        client_id = params.get('client_id') or os.environ.get('TEAMS_CLIENT_ID')
        client_secret = params.get('client_secret') or os.environ.get('TEAMS_CLIENT_SECRET')
        team_id = params.get('team_id')
        channel_id = params.get('channel_id')
        operation = params.get('operation', 'list_channels')

        if not all([tenant_id, client_id, client_secret]):
            return ActionResult(success=False, message="tenant_id, client_id, and client_secret are required")

        import urllib.request
        import urllib.error

        # Get access token
        try:
            token_req = urllib.request.Request(
                f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token',
                data=urllib.parse.urlencode({
                    'grant_type': 'client_credentials',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'scope': 'https://graph.microsoft.com/.default'
                }).encode('utf-8'),
                method='POST',
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            with urllib.request.urlopen(token_req, timeout=15) as resp:
                token_data = json.loads(resp.read().decode('utf-8'))
            access_token = token_data.get('access_token')
            if not access_token:
                return ActionResult(success=False, message="Failed to get access token")
        except Exception as e:
            return ActionResult(success=False, message=f"Auth error: {str(e)}")

        headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}

        try:
            if operation == 'list_channels':
                if not team_id:
                    return ActionResult(success=False, message="team_id is required for list_channels")
                req = urllib.request.Request(
                    f'https://graph.microsoft.com/v1.0/teams/{team_id}/channels',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                channels = result.get('value', [])
                return ActionResult(success=True, message=f"Found {len(channels)} channels", data={'channels': channels})

            elif operation == 'send_message':
                if not channel_id:
                    return ActionResult(success=False, message="channel_id is required for send_message")
                message = params.get('message', '')
                payload = {'body': {'contentType': 'text', 'content': message}}

                req = urllib.request.Request(
                    f'https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Message sent", data=result)

            elif operation == 'list_messages':
                if not channel_id:
                    return ActionResult(success=False, message="channel_id is required for list_messages")
                req = urllib.request.Request(
                    f'https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/messages',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                messages = result.get('value', [])
                return ActionResult(success=True, message=f"Found {len(messages)} messages", data={'messages': messages})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Teams API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Teams error: {str(e)}")


class TeamsMeetingAction(BaseAction):
    """Manage Microsoft Teams meetings.

    Supports creating, listing, and managing online meetings.
    """
    action_type = "teams_meeting"
    display_name = "Teams会议"
    description = "创建和管理Microsoft Teams会议"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Teams meetings.

        Args:
            context: Execution context.
            params: Dict with keys:
                - tenant_id: Azure AD tenant ID
                - client_id: Azure app client ID
                - client_secret: Azure app client secret
                - operation: create | list | get | cancel
                - user_id: User ID (for create/list)
                - meeting_id: Meeting ID (for get/cancel)
                - subject: Meeting subject
                - start_time: ISO datetime
                - end_time: ISO datetime
                - participants: List of email addresses

        Returns:
            ActionResult with meeting data.
        """
        import urllib.parse

        tenant_id = params.get('tenant_id') or os.environ.get('TEAMS_TENANT_ID')
        client_id = params.get('client_id') or os.environ.get('TEAMS_CLIENT_ID')
        client_secret = params.get('client_secret') or os.environ.get('TEAMS_CLIENT_SECRET')
        operation = params.get('operation', 'list')

        if not all([tenant_id, client_id, client_secret]):
            return ActionResult(success=False, message="tenant_id, client_id, and client_secret are required")

        import urllib.request
        import urllib.error

        # Get access token
        try:
            token_req = urllib.request.Request(
                f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token',
                data=urllib.parse.urlencode({
                    'grant_type': 'client_credentials',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'scope': 'https://graph.microsoft.com/.default'
                }).encode('utf-8'),
                method='POST',
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            with urllib.request.urlopen(token_req, timeout=15) as resp:
                token_data = json.loads(resp.read().decode('utf-8'))
            access_token = token_data.get('access_token')
        except Exception as e:
            return ActionResult(success=False, message=f"Auth error: {str(e)}")

        headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}

        try:
            if operation == 'create':
                user_id = params.get('user_id', 'me')
                payload = {
                    'subject': params.get('subject', 'Teams Meeting'),
                    'startDateTime': params.get('start_time', ''),
                    'endDateTime': params.get('end_time', ''),
                    'isOnlineMeeting': True,
                    'onlineMeetingProvider': 'teamsForBusiness',
                }
                if params.get('participants'):
                    payload['attendees'] = [
                        {'emailAddress': {'address': p}, 'type': 'required'}
                        for p in params['participants']
                    ]

                req = urllib.request.Request(
                    f'https://graph.microsoft.com/v1.0/users/{user_id}/events',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Meeting created", data=result)

            elif operation == 'list':
                user_id = params.get('user_id', 'me')
                req = urllib.request.Request(
                    f'https://graph.microsoft.com/v1.0/users/{user_id}/events?$filter=isOnlineMeeting eq true',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                meetings = result.get('value', [])
                return ActionResult(success=True, message=f"Found {len(meetings)} meetings", data={'meetings': meetings})

            elif operation == 'get':
                user_id = params.get('user_id', 'me')
                meeting_id = params.get('meeting_id')
                if not meeting_id:
                    return ActionResult(success=False, message="meeting_id is required")
                req = urllib.request.Request(
                    f'https://graph.microsoft.com/v1.0/users/{user_id}/events/{meeting_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Meeting retrieved", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Teams API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Teams error: {str(e)}")


class TeamsWebhookAction(BaseAction):
    """Manage Teams incoming webhooks.

    Enables creating and managing webhook connectors for channels.
    """
    action_type = "teams_webhook"
    display_name = "Teams Webhook管理"
    description = "创建和管理Teams频道Webhook"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Teams webhooks.

        Args:
            context: Execution context.
            params: Dict with keys:
                - tenant_id: Azure AD tenant ID
                - client_id: Azure app client ID
                - client_secret: Azure app client secret
                - team_id: Team ID
                - channel_id: Channel ID
                - operation: create_webhook | list_webhooks
                - webhook_name: Name for new webhook

        Returns:
            ActionResult with webhook data.
        """
        import urllib.parse

        tenant_id = params.get('tenant_id') or os.environ.get('TEAMS_TENANT_ID')
        client_id = params.get('client_id') or os.environ.get('TEAMS_CLIENT_ID')
        client_secret = params.get('client_secret') or os.environ.get('TEAMS_CLIENT_SECRET')
        operation = params.get('operation', 'list_webhooks')

        if not all([tenant_id, client_id, client_secret]):
            return ActionResult(success=False, message="tenant_id, client_id, and client_secret are required")

        import urllib.request
        import urllib.error

        try:
            token_req = urllib.request.Request(
                f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token',
                data=urllib.parse.urlencode({
                    'grant_type': 'client_credentials',
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'scope': 'https://graph.microsoft.com/.default'
                }).encode('utf-8'),
                method='POST',
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            with urllib.request.urlopen(token_req, timeout=15) as resp:
                token_data = json.loads(resp.read().decode('utf-8'))
            access_token = token_data.get('access_token')
        except Exception as e:
            return ActionResult(success=False, message=f"Auth error: {str(e)}")

        headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}

        try:
            team_id = params.get('team_id')
            channel_id = params.get('channel_id')

            if operation == 'list_webhooks':
                if not team_id or not channel_id:
                    return ActionResult(success=False, message="team_id and channel_id required")
                req = urllib.request.Request(
                    f'https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/webhooks',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                webhooks = result.get('value', [])
                return ActionResult(success=True, message=f"Found {len(webhooks)} webhooks", data={'webhooks': webhooks})

            elif operation == 'create_webhook':
                if not team_id or not channel_id:
                    return ActionResult(success=False, message="team_id and channel_id required")
                webhook_name = params.get('webhook_name', 'New Webhook')
                payload = {'name': webhook_name}

                req = urllib.request.Request(
                    f'https://graph.microsoft.com/v1.0/teams/{team_id}/channels/{channel_id}/webhooks',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Webhook created", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Teams API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Teams error: {str(e)}")
