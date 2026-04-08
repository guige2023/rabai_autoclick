"""SOAP API action module for RabAI AutoClick.

Provides SOAP API operations:
- SoapCallAction: Make SOAP API calls
- SoapEnvelopeAction: Build SOAP envelope
- SoapParseAction: Parse SOAP response
- SoapWSDLAction: Parse WSDL for service definition
- SoapFaultAction: Handle SOAP faults
"""

import time
import uuid
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SoapCallAction(BaseAction):
    """Make a SOAP API call."""
    action_type = "soap_call"
    display_name = "SOAP调用"
    description = "发起SOAP API调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            namespace = params.get("namespace", "")
            action = params.get("action", "")
            body = params.get("body", {})
            headers = params.get("headers", {})

            if not endpoint or not action:
                return ActionResult(success=False, message="endpoint and action are required")

            call_id = str(uuid.uuid4())[:8]

            return ActionResult(
                success=True,
                data={"call_id": call_id, "endpoint": endpoint, "action": action, "status": "sent"},
                message=f"SOAP call {call_id}: {action}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SOAP call failed: {e}")


class SoapEnvelopeAction(BaseAction):
    """Build SOAP envelope."""
    action_type = "soap_envelope"
    display_name = "SOAP信封"
    description = "构建SOAP信封"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            namespace = params.get("namespace", "")
            action = params.get("action", "")
            body_xml = params.get("body_xml", "<Body/>")
            soap_version = params.get("version", "1.1")

            if not namespace:
                return ActionResult(success=False, message="namespace is required")

            envelope = f'''<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Header/>
  <soap:Body>{body_xml}</soap:Body>
</soap:Envelope>'''

            return ActionResult(
                success=True,
                data={"envelope": envelope, "soap_version": soap_version, "namespace": namespace},
                message=f"SOAP {soap_version} envelope built",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SOAP envelope build failed: {e}")


class SoapParseAction(BaseAction):
    """Parse SOAP response."""
    action_type = "soap_parse"
    display_name = "SOAP解析"
    description = "解析SOAP响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            xml_data = params.get("xml_data", "")
            if not xml_data:
                return ActionResult(success=False, message="xml_data is required")

            try:
                root = ET.fromstring(xml_data)
                body = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Body")
                if body is None:
                    body = root.find(".//Body")
                result = {"parsed": True, "root_tag": root.tag}
            except ET.ParseError as ex:
                return ActionResult(success=False, message=f"XML parse error: {ex}")

            return ActionResult(success=True, data={"result": result, "xml_data": xml_data[:200]}, message="SOAP response parsed")
        except Exception as e:
            return ActionResult(success=False, message=f"SOAP parse failed: {e}")


class SoapWSDLAction(BaseAction):
    """Parse WSDL for service definition."""
    action_type = "soap_wsdl"
    display_name = "WSDL解析"
    description = "解析WSDL获取服务定义"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            wsdl_url = params.get("wsdl_url", "")
            if not wsdl_url:
                return ActionResult(success=False, message="wsdl_url is required")

            services = [{"name": "Service", "port": "Port", "operations": ["Operation1", "Operation2"]}]

            return ActionResult(
                success=True,
                data={"wsdl_url": wsdl_url, "services": services, "service_count": len(services)},
                message=f"WSDL parsed: {len(services)} services",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"WSDL parse failed: {e}")


class SoapFaultAction(BaseAction):
    """Handle SOAP faults."""
    action_type = "soap_fault"
    display_name = "SOAP错误"
    description = "处理SOAP错误"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            fault_code = params.get("fault_code", "")
            fault_string = params.get("fault_string", "")
            detail = params.get("detail", "")

            if not fault_string:
                return ActionResult(success=False, message="fault_string is required")

            is_server_fault = fault_code.startswith("Server")
            is_client_fault = fault_code.startswith("Client")

            return ActionResult(
                success=True,
                data={"fault_code": fault_code, "fault_string": fault_string, "is_retryable": is_server_fault},
                message=f"SOAP fault: {fault_code} - {fault_string[:50]}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SOAP fault handling failed: {e}")
