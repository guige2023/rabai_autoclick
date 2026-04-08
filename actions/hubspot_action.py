"""HubSpot action module for RabAI AutoClick.

Provides HubSpot CRM operations for contacts, deals, and tickets.
"""

import json
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class HubSpotAction(BaseAction):
    """HubSpot CRM operations.
    
    Supports managing contacts, deals, tickets, companies,
    and HubSpot workflow automation.
    """
    action_type = "hubspot"
    display_name = "HubSpot CRM"
    description = "HubSpot CRM联系人、交易与工单管理"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute HubSpot operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'create_contact', 'get_contact', 'list_contacts', 'update_contact', 'delete_contact'
                - api_key: HubSpot API key (or env HUBSPOT_API_KEY)
                - properties: Contact/deal properties dict
                - contact_id: Contact ID for get/update/delete
        
        Returns:
            ActionResult with operation result.
        """
        api_key = params.get('api_key') or os.environ.get('HUBSPOT_API_KEY')
        if not api_key:
            return ActionResult(success=False, message="HubSpot API key required (set HUBSPOT_API_KEY env)")
        
        command = params.get('command', 'list_contacts')
        properties = params.get('properties', {})
        contact_id = params.get('contact_id')
        deal_id = params.get('deal_id')
        
        base_url = "https://api.hubapi.com"
        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
        
        if command == 'create_contact':
            return self._hubspot_create(base_url, headers, 'contacts', properties)
        
        if command == 'get_contact':
            if not contact_id:
                return ActionResult(success=False, message="contact_id required for get_contact")
            return self._hubspot_get(base_url, headers, f'contacts/{contact_id}', {'properties': 'email,firstname,lastname,phone,company'})
        
        if command == 'list_contacts':
            limit = params.get('limit', 10)
            return self._hubspot_list(base_url, headers, 'contacts', limit)
        
        if command == 'update_contact':
            if not contact_id:
                return ActionResult(success=False, message="contact_id required for update_contact")
            return self._hubspot_update(base_url, headers, f'contacts/{contact_id}', properties)
        
        if command == 'delete_contact':
            if not contact_id:
                return ActionResult(success=False, message="contact_id required for delete_contact")
            return self._hubspot_delete(base_url, headers, f'contacts/{contact_id}')
        
        if command == 'create_deal':
            return self._hubspot_create(base_url, headers, 'deals', properties)
        
        if command == 'create_company':
            return self._hubspot_create(base_url, headers, 'companies', properties)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _hubspot_create(self, base_url: str, headers: Dict, endpoint: str, properties: Dict) -> ActionResult:
        """Create HubSpot record."""
        from urllib.request import Request, urlopen
        
        try:
            data = json.dumps({'properties': properties}).encode('utf-8')
            request = Request(f"{base_url}/crm/v3/objects/{endpoint}", data=data, headers=headers, method='POST')
            with urlopen(request, timeout=15) as resp:
                result = json.loads(resp.read().decode())
                record_id = result.get('id', '')
                return ActionResult(
                    success=True,
                    message=f"Created {endpoint[:-1]} {record_id}",
                    data={'id': record_id, 'properties': result.get('properties', {})}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to create: {e}")
    
    def _hubspot_get(self, base_url: str, headers: Dict, endpoint: str, properties: str) -> ActionResult:
        """Get HubSpot record."""
        from urllib.request import Request, urlopen
        
        try:
            url = f"{base_url}/crm/v3/objects/{endpoint}?properties={properties}"
            request = Request(url, headers=headers, method='GET')
            with urlopen(request, timeout=15) as resp:
                result = json.loads(resp.read().decode())
                return ActionResult(
                    success=True,
                    message=f"Retrieved {endpoint.split('/')[0]}",
                    data={'id': result.get('id'), 'properties': result.get('properties', {})}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to get: {e}")
    
    def _hubspot_list(self, base_url: str, headers: Dict, endpoint: str, limit: int) -> ActionResult:
        """List HubSpot records."""
        from urllib.request import Request, urlopen
        
        try:
            url = f"{base_url}/crm/v3/objects/{endpoint}?limit={limit}&properties=email,firstname,lastname,phone,company"
            request = Request(url, headers=headers, method='GET')
            with urlopen(request, timeout=15) as resp:
                result = json.loads(resp.read().decode())
                records = result.get('results', [])
                return ActionResult(
                    success=True,
                    message=f"Listed {len(records)} {endpoint}",
                    data={'contacts': [{'id': r.get('id'), 'properties': r.get('properties', {})} for r in records], 'count': len(records)}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to list: {e}")
    
    def _hubspot_update(self, base_url: str, headers: Dict, endpoint: str, properties: Dict) -> ActionResult:
        """Update HubSpot record."""
        from urllib.request import Request, urlopen
        
        try:
            data = json.dumps({'properties': properties}).encode('utf-8')
            request = Request(f"{base_url}/crm/v3/objects/{endpoint}", data=data, headers=headers, method='PATCH')
            with urlopen(request, timeout=15) as resp:
                result = json.loads(resp.read().decode())
                return ActionResult(
                    success=True,
                    message=f"Updated {endpoint.split('/')[0]} {result.get('id')}",
                    data={'id': result.get('id'), 'properties': result.get('properties', {})}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to update: {e}")
    
    def _hubspot_delete(self, base_url: str, headers: Dict, endpoint: str) -> ActionResult:
        """Delete HubSpot record."""
        from urllib.request import Request, urlopen
        
        try:
            request = Request(f"{base_url}/crm/v3/objects/{endpoint}", headers=headers, method='DELETE')
            with urlopen(request, timeout=15) as resp:
                return ActionResult(success=True, message=f"Deleted {endpoint}")
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to delete: {e}")
