"""HubSpot integration for RabAI AutoClick.

Provides actions to manage CRM contacts, deals, companies, and tickets in HubSpot.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HubSpotContactAction(BaseAction):
    """Manage HubSpot contacts - create, update, search contacts.

    Uses HubSpot CRM API v3.
    """
    action_type = "hubspot_contact"
    display_name = "HubSpot联系人"
    description = "管理HubSpot CRM联系人"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage HubSpot contacts.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: HubSpot API key (or private app token)
                - operation: create | update | get | delete | list | search
                - contact_id: Contact ID (for update/get/delete)
                - email: Contact email
                - firstname: First name
                - lastname: Last name
                - properties: Dict of HubSpot properties
                - limit: Max results (default 10)

        Returns:
            ActionResult with contact data.
        """
        api_key = params.get('api_key') or os.environ.get('HUBSPOT_API_KEY')
        operation = params.get('operation', 'list')

        if not api_key:
            return ActionResult(success=False, message="HUBSPOT_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }

        try:
            if operation == 'create':
                properties = params.get('properties', {})
                if not properties:
                    for key in ['email', 'firstname', 'lastname', 'phone', 'company']:
                        if key in params:
                            properties[key] = params[key]
                if not properties.get('email'):
                    return ActionResult(success=False, message="email is required for create")

                req = urllib.request.Request(
                    'https://api.hubapi.com/crm/v3/objects/contacts',
                    data=json.dumps({'properties': properties}).encode('utf-8'),
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

                properties = params.get('properties', {})
                req = urllib.request.Request(
                    f'https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}',
                    data=json.dumps({'properties': properties}).encode('utf-8'),
                    method='PATCH',
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
                    f'https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Contact retrieved", data=result)

            elif operation == 'delete':
                contact_id = params.get('contact_id')
                if not contact_id:
                    return ActionResult(success=False, message="contact_id is required for delete")
                req = urllib.request.Request(
                    f'https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}',
                    method='DELETE',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Contact deleted", data=result)

            elif operation == 'list':
                limit = params.get('limit', 10)
                after = params.get('after', '')
                url = f'https://api.hubapi.com/crm/v3/objects/contacts?limit={limit}'
                if after:
                    url += f'&after={after}'
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                contacts = result.get('results', [])
                return ActionResult(
                    success=True,
                    message=f"Found {len(contacts)} contacts",
                    data={'contacts': contacts, 'paging': result.get('paging')}
                )

            elif operation == 'search':
                query = params.get('query', '')
                filter_obj = params.get('filter', {})
                if not filter_obj and query:
                    filter_obj = {'propertyName': 'email', 'operator': 'CONTAINS', 'value': query}

                payload = {
                    'filterGroups': [{
                        'filters': [filter_obj]
                    }],
                    'limit': params.get('limit', 10),
                }

                req = urllib.request.Request(
                    'https://api.hubapi.com/crm/v3/objects/contacts/search',
                    data=json.dumps(payload).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                contacts = result.get('results', [])
                return ActionResult(success=True, message=f"Found {len(contacts)} contacts", data={'contacts': contacts})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"HubSpot API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"HubSpot error: {str(e)}")


class HubSpotDealAction(BaseAction):
    """Manage HubSpot deals - create, update, and manage deal stages.

    Supports deal pipelines and associated contacts.
    """
    action_type = "hubspot_deal"
    display_name = "HubSpot交易"
    description = "管理HubSpot CRM交易和管道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage HubSpot deals.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: HubSpot API key
                - operation: create | update | get | delete | list | add_contact
                - deal_id: Deal ID (for update/get/delete/add_contact)
                - dealname: Deal name
                - amount: Deal amount
                - pipeline: Pipeline ID
                - dealstage: Deal stage ID
                - closedate: Close date (ISO)
                - properties: Dict of additional properties
                - contact_id: Contact ID to associate

        Returns:
            ActionResult with deal data.
        """
        api_key = params.get('api_key') or os.environ.get('HUBSPOT_API_KEY')
        operation = params.get('operation', 'list')

        if not api_key:
            return ActionResult(success=False, message="HUBSPOT_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }

        try:
            if operation == 'create':
                properties = params.get('properties', {})
                if not properties:
                    for key in ['dealname', 'amount', 'pipeline', 'dealstage', 'closedate']:
                        if params.get(key):
                            properties[key] = params[key]
                if not properties.get('dealname'):
                    return ActionResult(success=False, message="dealname is required for create")

                req = urllib.request.Request(
                    'https://api.hubapi.com/crm/v3/objects/deals',
                    data=json.dumps({'properties': properties}).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Deal created", data=result)

            elif operation == 'update':
                deal_id = params.get('deal_id')
                if not deal_id:
                    return ActionResult(success=False, message="deal_id is required for update")

                properties = params.get('properties', {})
                req = urllib.request.Request(
                    f'https://api.hubapi.com/crm/v3/objects/deals/{deal_id}',
                    data=json.dumps({'properties': properties}).encode('utf-8'),
                    method='PATCH',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Deal updated", data=result)

            elif operation == 'get':
                deal_id = params.get('deal_id')
                if not deal_id:
                    return ActionResult(success=False, message="deal_id is required for get")
                req = urllib.request.Request(
                    f'https://api.hubapi.com/crm/v3/objects/deals/{deal_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Deal retrieved", data=result)

            elif operation == 'list':
                limit = params.get('limit', 10)
                req = urllib.request.Request(
                    f'https://api.hubapi.com/crm/v3/objects/deals?limit={limit}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                deals = result.get('results', [])
                return ActionResult(success=True, message=f"Found {len(deals)} deals", data={'deals': deals})

            elif operation == 'add_contact':
                deal_id = params.get('deal_id')
                contact_id = params.get('contact_id')
                if not deal_id or not contact_id:
                    return ActionResult(success=False, message="deal_id and contact_id are required")

                payload = {
                    'to': [{'id': deal_id}],
                    'types': [{'associationCategory': 'HUBSPOT_DEFINED', 'associationTypeId': 3}]
                }
                req = urllib.request.Request(
                    f'https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}/associations/deals',
                    data=json.dumps(payload).encode('utf-8'),
                    method='PUT',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Contact associated with deal", data=result)

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"HubSpot API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"HubSpot error: {str(e)}")


class HubSpotTicketAction(BaseAction):
    """Manage HubSpot tickets and support pipelines.

    Handles ticket creation, status updates, and pipeline management.
    """
    action_type = "hubspot_ticket"
    display_name = "HubSpot工单"
    description = "管理HubSpot支持工单"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Manage HubSpot tickets.

        Args:
            context: Execution context.
            params: Dict with keys:
                - api_key: HubSpot API key
                - operation: create | update | get | list | add_pipeline_note
                - ticket_id: Ticket ID
                - subject: Ticket subject
                - content: Ticket description
                - pipeline: Pipeline ID
                - hs_ticket_priority: LOW | MEDIUM | HIGH | URGENT
                - properties: Dict of additional properties

        Returns:
            ActionResult with ticket data.
        """
        api_key = params.get('api_key') or os.environ.get('HUBSPOT_API_KEY')
        operation = params.get('operation', 'list')

        if not api_key:
            return ActionResult(success=False, message="HUBSPOT_API_KEY is required")

        import urllib.request
        import urllib.error

        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }

        try:
            if operation == 'create':
                properties = params.get('properties', {})
                if not properties:
                    properties = {
                        'subject': params.get('subject', ''),
                        'content': params.get('content', ''),
                    }
                    for key in ['pipeline', 'hs_ticket_priority']:
                        if params.get(key):
                            properties[key] = params[key]

                req = urllib.request.Request(
                    'https://api.hubapi.com/crm/v3/objects/tickets',
                    data=json.dumps({'properties': properties}).encode('utf-8'),
                    method='POST',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Ticket created", data=result)

            elif operation == 'update':
                ticket_id = params.get('ticket_id')
                if not ticket_id:
                    return ActionResult(success=False, message="ticket_id is required for update")

                properties = params.get('properties', {})
                req = urllib.request.Request(
                    f'https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}',
                    data=json.dumps({'properties': properties}).encode('utf-8'),
                    method='PATCH',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Ticket updated", data=result)

            elif operation == 'get':
                ticket_id = params.get('ticket_id')
                if not ticket_id:
                    return ActionResult(success=False, message="ticket_id is required for get")
                req = urllib.request.Request(
                    f'https://api.hubapi.com/crm/v3/objects/tickets/{ticket_id}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                return ActionResult(success=True, message="Ticket retrieved", data=result)

            elif operation == 'list':
                limit = params.get('limit', 10)
                req = urllib.request.Request(
                    f'https://api.hubapi.com/crm/v3/objects/tickets?limit={limit}',
                    headers=headers
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode('utf-8'))
                tickets = result.get('results', [])
                return ActionResult(success=True, message=f"Found {len(tickets)} tickets", data={'tickets': tickets})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except urllib.error.HTTPError as e:
            body = e.read().decode('utf-8')
            return ActionResult(success=False, message=f"HubSpot API error: {e.code}", data={'body': body[:500]})
        except Exception as e:
            return ActionResult(success=False, message=f"HubSpot error: {str(e)}")
