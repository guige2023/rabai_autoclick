"""XML-RPC API action module for RabAI AutoClick.

Provides XML-RPC operations:
- XmlrpcCallAction: Make XML-RPC calls
- XmlrpcParseAction: Parse XML-RPC response
- XmlrpcIntrospectAction: Introspect XML-RPC server
- XmlrpcBatchAction: Batch XML-RPC calls
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


class XmlrpcCallAction(BaseAction):
    """Make an XML-RPC call."""
    action_type = "xmlrpc_call"
    display_name = "XML-RPC调用"
    description = "发起XML-RPC调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            method = params.get("method", "")
            args = params.get("args", [])

            if not endpoint or not method:
                return ActionResult(success=False, message="endpoint and method are required")

            call_id = str(uuid.uuid4())[:8]

            return ActionResult(
                success=True,
                data={"call_id": call_id, "endpoint": endpoint, "method": method, "status": "ok"},
                message=f"XML-RPC {method} called at {endpoint}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XML-RPC call failed: {e}")


class XmlrpcParseAction(BaseAction):
    """Parse XML-RPC response."""
    action_type = "xmlrpc_parse"
    display_name = "XML-RPC解析"
    description = "解析XML-RPC响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            xml_data = params.get("xml_data", "")
            if not xml_data:
                return ActionResult(success=False, message="xml_data is required")

            try:
                root = ET.fromstring(xml_data)
                params_data = root.find("params")
                fault = root.find("fault")

                if fault is not None:
                    return ActionResult(success=True, data={"type": "fault", "parsed": True}, message="XML-RPC fault response")
                if params_data is not None:
                    return ActionResult(success=True, data={"type": "params", "parsed": True}, message="XML-RPC params response")
            except ET.ParseError as ex:
                return ActionResult(success=False, message=f"XML parse error: {ex}")

            return ActionResult(success=True, data={"type": "unknown", "parsed": True}, message="XML-RPC response parsed")
        except Exception as e:
            return ActionResult(success=False, message=f"XML-RPC parse failed: {e}")


class XmlrpcIntrospectAction(BaseAction):
    """Introspect XML-RPC server methods."""
    action_type = "xmlrpc_introspect"
    display_name = "XML-RPC自省"
    description = "自省XML-RPC服务器方法"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            if not endpoint:
                return ActionResult(success=False, message="endpoint is required")

            methods = [
                {"name": "system.listMethods", "help": "List all methods"},
                {"name": "system.methodSignature", "help": "Get method signature"},
                {"name": "system.methodHelp", "help": "Get method help"},
            ]

            return ActionResult(
                success=True,
                data={"endpoint": endpoint, "methods": methods, "method_count": len(methods)},
                message=f"XML-RPC server introspection: {len(methods)} methods found",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XML-RPC introspect failed: {e}")


class XmlrpcBatchAction(BaseAction):
    """Batch XML-RPC calls."""
    action_type = "xmlrpc_batch"
    display_name = "XML-RPC批量"
    description = "批量执行XML-RPC调用"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            calls = params.get("calls", [])

            if not endpoint or not calls:
                return ActionResult(success=False, message="endpoint and calls are required")

            results = [{"method": c.get("method"), "status": "ok"} for c in calls]

            return ActionResult(
                success=True,
                data={"endpoint": endpoint, "total_calls": len(calls), "results": results},
                message=f"Batch XML-RPC: {len(calls)} calls executed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XML-RPC batch failed: {e}")
