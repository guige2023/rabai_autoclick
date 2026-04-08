"""SOAP action module for RabAI AutoClick.

Provides SOAP web service client operations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SOAPAction(BaseAction):
    """SOAP web service client.
    
    Supports calling SOAP web services with WSDL parsing,
    envelope generation, and XML response parsing.
    """
    action_type = "soap"
    display_name = "SOAP客户端"
    description = "SOAP WebService客户端调用"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SOAP operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'call', 'parse_wsdl'
                - wsdl_url: WSDL endpoint URL
                - service_url: SOAP service endpoint URL
                - action: SOAP action/method name
                - namespace: SOAP namespace
                - body: Dict of parameters for the SOAP call
                - soap_version: '1.1' or '1.2' (default '1.1')
                - headers: Additional SOAP headers
        
        Returns:
            ActionResult with operation result.
        """
        command = params.get('command', 'call')
        wsdl_url = params.get('wsdl_url')
        service_url = params.get('service_url')
        action = params.get('action')
        namespace = params.get('namespace', 'http://tempuri.org/')
        body = params.get('body', {})
        soap_version = params.get('soap_version', '1.1')
        headers = params.get('headers', {})
        
        if command == 'call':
            if not service_url or not action:
                return ActionResult(success=False, message="service_url and action required for call")
            return self._soap_call(service_url, action, namespace, body, soap_version, headers)
        
        if command == 'parse_wsdl':
            if not wsdl_url:
                return ActionResult(success=False, message="wsdl_url required for parse_wsdl")
            return self._parse_wsdl(wsdl_url)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _soap_call(self, service_url: str, action: str, namespace: str, body: Dict, soap_version: str, headers: Dict) -> ActionResult:
        """Make SOAP call."""
        import xml.etree.ElementTree as ET
        from urllib.request import Request, urlopen
        from urllib.error import URLError, HTTPError
        
        ns = {
            'soap11': 'http://schemas.xmlsoap.org/soap/envelope/',
            'soap12': 'http://www.w3.org/2003/05/soap-envelope',
        }
        
        if soap_version == '1.2':
            envelope_ns = ns['soap12']
            content_type = 'application/soap+xml; charset=utf-8'
        else:
            envelope_ns = ns['soap11']
            content_type = 'text/xml; charset=utf-8'
        
        envelope = ET.Element('soap:Envelope' if soap_version == '1.1' else 's12:Envelope',
                              attrib={'xmlns:soap': envelope_ns, 'xmlns:tns': namespace})
        header_elem = ET.SubElement(envelope, 'soap:Header' if soap_version == '1.1' else 's12:Header')
        body_elem = ET.SubElement(envelope, 'soap:Body' if soap_version == '1.1' else 's12:Body')
        
        action_elem = ET.SubElement(body_elem, f'tns:{action}', attrib={'xmlns:tns': namespace})
        for key, value in body.items():
            param = ET.SubElement(action_elem, key)
            param.text = str(value)
        
        for key, value in headers.items():
            header_item = ET.SubElement(header_elem, key)
            header_item.text = str(value)
        
        xml_str = ET.tostring(envelope, encoding='unicode')
        xml_str = '<?xml version="1.0" encoding="utf-8"?>' + xml_str
        
        req_headers = {
            'Content-Type': content_type,
            'SOAPAction': f'"{namespace}/{action}"' if not namespace.endswith('/') else f'"{namespace}{action}"',
        }
        
        try:
            request = Request(service_url, data=xml_str.encode('utf-8'), headers=req_headers, method='POST')
            with urlopen(request, timeout=30) as resp:
                response_xml = resp.read().decode('utf-8')
            
            try:
                root = ET.fromstring(response_xml)
                body_result = self._extract_soap_body(root, soap_version)
                return ActionResult(
                    success=True,
                    message=f"SOAP call {action} succeeded",
                    data={'result': body_result, 'raw_xml': response_xml[:2000]}
                )
            except ET.ParseError:
                return ActionResult(
                    success=True,
                    message=f"SOAP call {action} succeeded",
                    data={'raw_xml': response_xml[:2000]}
                )
        except HTTPError as e:
            try:
                error_body = e.read().decode('utf-8')
                return ActionResult(success=False, message=f"SOAP error {e.code}: {error_body[:500]}")
            except Exception:
                return ActionResult(success=False, message=f"SOAP HTTP error: {e.code}")
        except Exception as e:
            return ActionResult(success=False, message=f"SOAP call failed: {e}")
    
    def _extract_soap_body(self, root: Any, soap_version: str) -> Dict[str, Any]:
        """Extract data from SOAP body."""
        import xml.etree.ElementTree as ET
        ns = {
            'soap11': 'http://schemas.xmlsoap.org/soap/envelope/',
            'soap12': 'http://www.w3.org/2003/05/soap-envelope',
        }
        ns_key = 'soap11' if soap_version == '1.1' else 'soap12'
        body_tag = f'{{{ns[ns_key]}}}Body'
        
        body_elem = root.find(body_tag)
        if body_elem is None:
            return {}
        
        result = {}
        for child in body_elem:
            result[child.tag.split('}')[1] if '}' in child.tag else child.tag] = child.text or {}
            for subchild in child:
                result[child.tag.split('}')[1] if '}' in child.tag]][subchild.tag.split('}')[1] if '}' in subchild.tag else subchild.tag] = subchild.text
        
        return result if result else (body_elem.text or {})
    
    def _parse_wsdl(self, wsdl_url: str) -> ActionResult:
        """Parse WSDL and extract service info."""
        from urllib.request import urlopen
        import xml.etree.ElementTree as ET
        
        try:
            with urlopen(wsdl_url, timeout=10) as resp:
                wsdl_xml = resp.read().decode('utf-8')
            
            root = ET.fromstring(wsdl_xml)
            namespaces = {
                'wsdl': 'http://schemas.xmlsoap.org/wsdl/',
                'soap': 'http://schemas.xmlsoap.org/wsdl/soap/',
                'tns': root.get('targetNamespace', ''),
            }
            
            services = []
            for service in root.findall('wsdl:service', namespaces):
                service_name = service.get('name', 'Unknown')
                ports = []
                for port in service.findall('wsdl:port', namespaces):
                    port_name = port.get('name', '')
                    binding = port.get('binding', '').split(':')[-1]
                    soap_addr = port.find('soap:address', namespaces)
                    location = soap_addr.get('location', '') if soap_addr is not None else ''
                    ports.append({'name': port_name, 'binding': binding, 'location': location})
                services.append({'name': service_name, 'ports': ports})
            
            port_types = [pt.get('name') for pt in root.findall('wsdl:portType', namespaces) if pt.get('name')]
            operations = []
            for pt in root.findall('wsdl:portType', namespaces):
                for op in pt.findall('wsdl:operation', namespaces):
                    operations.append({
                        'port_type': pt.get('name'),
                        'name': op.get('name'),
                        'input': (op.find('wsdl:input', namespaces).get('message', '').split(':')[-1] if op.find('wsdl:input', namespaces) is not None else None),
                        'output': (op.find('wsdl:output', namespaces).get('message', '').split(':')[-1] if op.find('wsdl:output', namespaces) is not None else None),
                    })
            
            return ActionResult(
                success=True,
                message=f"WSDL parsed: {len(services)} services, {len(operations)} operations",
                data={
                    'services': services,
                    'port_types': port_types,
                    'operations': operations,
                    'namespace': namespaces['tns']
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Failed to parse WSDL: {e}")
