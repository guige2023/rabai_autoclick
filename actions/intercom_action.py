"""Intercom integration for RabAI AutoClick.

Provides actions to manage conversations, contacts, and messages in Intercom.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class IntercomConversationAction(BaseAction):
    """Manage Intercom conversations - reply, assign, close.

    Supports conversation lifecycle management and message threading.
    """
    action_type = "intercom_conversation"
    display_name = "Intercom对话"
    description = "管理Intercom对话：回复、分配、关闭"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Intercom conversations.

        Args:
            context: Execution context.
            params: Dict with keys:
                - access_token: Intercom access token
                - operation: reply | assign | close | open | get | list
                - conversation_id: Conversation ID
                - message: Reply message body
                - message_type: comment | note
                - admin_id: Admin ID for assigning
                - assignee_id: Admin ID to assign to
                - body: Message body (for reply)
                - customer_email: Customer email (for initiating)

        Returns:
            ActionResult with conversation data.
        """
        access_token = params.get('access_token') or os.environ.get('INTERCOM_ACCESS_TOKEN')
        operation = params.get('operation', 'list')

        if not access_token:
            return ActionResult(success=False, message="INTERCOM_ACCESS_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            if operation == 'reply':
                conversation_id = params.get('conversation_id')
                if not conversation_id:
                    return ActionResult(success=False, message="conversation_id is required for reply")

                payload = {
                    'message_type': params.get('message_type', 'comment'),
                    'type': 'admin',
                    'admin_id': params.get('admin_id', ''),
                    'body': params.get('body', ''),
                }
                if params.get('attachment_urls'):
                    payload['attachments'] = [{'type': 'upload', 'url': u} for u in params['attachment_urls']]

                req = urllib.request.Request(
                    f'https://api.intercom.io/conversations/{conversation_id}/reply',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Reply sent", data=result)

            elif operation == 'assign':
                conversation_id = params.get('conversation_id')
                if not conversation_id:
                    return ActionResult(success=False, message="conversation_id is required for assign")

                payload = {
                    'type': 'admin',
                    'admin_id': str(params.get('assignee_id', '')),
                    'message_type': 'assignment',
                }

                req = urllib.request.Request(
                    f'https://api.intercom.io/conversations/{conversation_id}/parts',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Conversation assigned", data=result)

            elif operation == 'close':
                conversation_id = params.get('conversation_id')
                if not conversation_id:
                    return ActionResult(success=False, message="conversation_id is required for close")

                payload = {'message_type': 'close', 'type': 'admin', 'admin_id': params.get('admin_id', '')}

                req = urllib.request.Request(
                    f'https://api.intercom.io/conversations/{conversation_id}/parts',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Conversation closed", data=result)

            elif operation == 'open':
                conversation_id = params.get('conversation_id')
                if not conversation_id:
                    return ActionResult(success=False, message="conversation_id is required for open")

                payload = {'message_type': 'open', 'type': 'admin', 'admin_id': params.get('admin_id', '')}

                req = urllib.request.Request(
                    f'https://api.intercom.io/conversations/{conversation_id}/parts',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Conversation opened", data=result)

            elif operation == 'get':
                conversation_id = params.get('conversation_id')
                if not conversation_id:
                    return ActionResult(success=False, message="conversation_id is required for get")
                req = urllib.request.Request(
                    f'https://api.intercom.io/conversations/{conversation_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Conversation retrieved", data=result)

            elif operation == 'list':
                url = 'https://api.intercom.io/conversations?'
                if params.get('state'):
                    url += f"state={params['state']}&"
                if params.get('query'):
                    url += f"query={params['query']}&"
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                convos = result.get('conversations', [])
                return ActionResult(success=True, message=f"Found {len(convos)} conversations", data={'conversations': convos})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Intercom API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Intercom error: {str(e)}")


class IntercomContactAction(BaseAction):
    """Manage Intercom contacts - create, update, list contacts.

    Handles contact lifecycle and attribute management.
    """
    action_type = "intercom_contact"
    display_name = "Intercom联系人"
    description = "管理Intercom联系人：创建、更新、列表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage Intercom contacts.

        Args:
            context: Execution context.
            params: Dict with keys:
                - access_token: Intercom access token
                - operation: create | update | get | list | delete | search
                - contact_id: Contact ID (for update/get/delete)
                - email: Contact email
                - name: Contact name
                - custom_attributes: Dict of custom field values
                - phone: Phone number
                - role: lead | user

        Returns:
            ActionResult with contact data.
        """
        access_token = params.get('access_token') or os.environ.get('INTERCOM_ACCESS_TOKEN')
        operation = params.get('operation', 'list')

        if not access_token:
            return ActionResult(success=False, message="INTERCOM_ACCESS_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            if operation == 'create':
                payload = {'role': params.get('role', 'user')}
                for key in ['email', 'name', 'phone', 'custom_attributes']:
                    if key in params and params[key]:
                        payload[key] = params[key]

                req = urllib.request.Request(
                    'https://api.intercom.io/contacts',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Contact created", data=result)

            elif operation == 'update':
                contact_id = params.get('contact_id')
                if not contact_id:
                    return ActionResult(success=False, message="contact_id is required for update")

                payload = {}
                for key in ['email', 'name', 'phone', 'custom_attributes']:
                    if key in params and params[key]:
                        payload[key] = params[key]

                req = urllib.request.Request(
                    f'https://api.intercom.io/contacts/{contact_id}',
                    data=json.dumps(payload).encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Contact updated", data=result)

            elif operation == 'get':
                contact_id = params.get('contact_id')
                if not contact_id:
                    return ActionResult(success=False, message="contact_id is required for get")
                req = urllib.request.Request(
                    f'https://api.intercom.io/contacts/{contact_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Contact retrieved", data=result)

            elif operation == 'list':
                req = urllib.request.Request('https://api.intercom.io/contacts', headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                contacts = result.get('data', [])
                return ActionResult(success=True, message=f"Found {len(contacts)} contacts", data={'contacts': contacts})

            elif operation == 'delete':
                contact_id = params.get('contact_id')
                if not contact_id:
                    return ActionResult(success=False, message="contact_id is required for delete")
                req = urllib.request.Request(
                    f'https://api.intercom.io/contacts/{contact_id}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Contact deleted", data=result)

            elif operation == 'search':
                query = params.get('query', {})
                payload = {
                    'query': {
                        'field': query.get('field', 'email'),
                        'operator': query.get('operator', '~'),
                        'value': query.get('value', '')
                    }
                }
                req = urllib.request.Request(
                    'https://api.intercom.io/contacts/search',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                contacts = result.get('data', [])
                return ActionResult(success=True, message=f"Found {len(contacts)} contacts", data={'contacts': contacts})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Intercom API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Intercom error: {str(e)}")


class IntercomMessageAction(BaseAction):
    """Send outbound messages via Intercom.

    Supports sending messages to contacts via email, push, and in-app.
    """
    action_type = "intercom_message"
    display_name = "Intercom消息发送"
    description = "通过Intercom发送出站消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Send Intercom messages.

        Args:
            context: Execution context.
            params: Dict with keys:
                - access_token: Intercom access token
                - operation: send | schedule | cancel
                - message_type: email | in_app | push | telegram
                - subject: Email subject
                - body: Message body
                - to: Recipient contact ID or email
                - from_type: admin or user
                - from_id: Admin or user ID
                - template: generic | personal
                - schedule_at: Unix timestamp for scheduled messages

        Returns:
            ActionResult with message result.
        """
        access_token = params.get('access_token') or os.environ.get('INTERCOM_ACCESS_TOKEN')
        operation = params.get('operation', 'send')

        if not access_token:
            return ActionResult(success=False, message="INTERCOM_ACCESS_TOKEN is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        try:
            if operation == 'send':
                message_type = params.get('message_type', 'email')
                payload = {
                    'message_type': message_type,
                    'subject': params.get('subject', ''),
                    'body': params.get('body', ''),
                    'to': {
                        'type': 'contact',
                        'id': params.get('contact_id', '')
                    },
                    'from': {
                        'type': params.get('from_type', 'admin'),
                        'id': params.get('from_id', ''),
                    }
                }
                if params.get('template'):
                    payload['template'] = params['template']

                req = urllib.request.Request(
                    'https://api.intercom.io/messages',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Message sent", data=result)

            elif operation == 'schedule':
                schedule_at = params.get('schedule_at')
                if not schedule_at:
                    return ActionResult(success=False, message="schedule_at is required for schedule")

                message_type = params.get('message_type', 'email')
                payload = {
                    'message_type': message_type,
                    'subject': params.get('subject', ''),
                    'body': params.get('body', ''),
                    'to': {'type': 'contact', 'id': params.get('contact_id', '')},
                    'from': {'type': params.get('from_type', 'admin'), 'id': params.get('from_id', '')},
                    'schedule_at': schedule_at,
                }

                req = urllib.request.Request(
                    'https://api.intercom.io/messages/schedule',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Message scheduled", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"Intercom API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"Intercom error: {str(e)}")
