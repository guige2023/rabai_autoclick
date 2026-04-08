"""Adapter action module for RabAI AutoClick.

Provides adapter pattern implementation:
- Adapter: Abstract adapter interface
- ObjectAdapter: Object-based adapter
- ClassAdapter: Class-based adapter
- LegacyAdapter: Legacy system adapter
"""

from typing import Any, Callable, Dict, List, Optional
from abc import ABC, abstractmethod
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Target(ABC):
    """Abstract target interface."""

    @abstractmethod
    def request(self, data: Any) -> Any:
        """Handle request."""
        pass


class Adaptee:
    """The class that needs adapting."""

    def __init__(self):
        self._legacy_data: Dict[str, Any] = {}

    def specific_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Legacy specific request."""
        action = params.get("action", "")
        value = params.get("value")

        if action == "get":
            return {"result": self._legacy_data.get(value)}
        elif action == "set":
            key = params.get("key", "")
            self._legacy_data[key] = value
            return {"success": True}
        elif action == "delete":
            key = params.get("key", "")
            if key in self._legacy_data:
                del self._legacy_data[key]
            return {"success": True}
        elif action == "list":
            return {"result": list(self._legacy_data.keys())}

        return {"error": "Unknown action"}


class ObjectAdapter(Target):
    """Object adapter wrapping Adaptee."""

    def __init__(self, adaptee: Adaptee):
        self._adaptee = adaptee

    def request(self, data: Any) -> Any:
        """Adapt and forward request."""
        if isinstance(data, dict):
            result = self._adaptee.specific_request(data)
            return self._adapt_response(result)
        return {"error": "Invalid request format"}

    def _adapt_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Adapt the response format."""
        return {
            "success": response.get("success", response.get("error") is None),
            "data": response.get("result"),
            "message": response.get("error", "OK"),
        }


class DataAdapter(Target):
    """Adapter for data format conversion."""

    def __init__(self):
        self._adaptees: Dict[str, Callable] = {}

    def register_adapter(self, format_name: str, adapter_fn: Callable) -> None:
        """Register an adapter function."""
        self._adaptees[format_name] = adapter_fn

    def request(self, data: Any) -> Any:
        """Process data with appropriate adapter."""
        if isinstance(data, dict):
            format_name = data.get("format", "default")
            content = data.get("content")

            adapter_fn = self._adaptees.get(format_name)
            if adapter_fn:
                return adapter_fn(content)
            return {"error": f"No adapter for format: {format_name}"}
        return {"error": "Invalid request format"}


class APIClient:
    """External API client."""

    def __init__(self):
        self._base_url = ""
        self._headers = {}

    def call(self, endpoint: str, method: str = "GET", data: Any = None) -> Dict:
        """Make API call."""
        return {
            "endpoint": endpoint,
            "method": method,
            "data": data,
            "status": "simulated",
        }


class APIClientAdapter(Target):
    """Adapter for API client."""

    def __init__(self, client: APIClient):
        self._client = client

    def request(self, data: Any) -> Any:
        """Adapt API request."""
        if isinstance(data, dict):
            endpoint = data.get("endpoint", "/")
            method = data.get("method", "GET")
            body = data.get("body")

            result = self._client.call(endpoint, method, body)

            return {
                "success": True,
                "response": result,
            }
        return {"success": False, "error": "Invalid request"}


class LegacySystem:
    """Legacy system with old interface."""

    def __init__(self):
        self._records: List[Dict] = []

    def add_record(self, record: Dict) -> None:
        """Add record in legacy format."""
        self._records.append({
            "id": record.get("ID", record.get("id", 0)),
            "name": record.get("NAME", record.get("name", "")),
            "value": record.get("VAL", record.get("value", 0)),
        })

    def find_records(self, criteria: Dict) -> List[Dict]:
        """Find records matching criteria."""
        results = []
        for record in self._records:
            match = True
            for key, value in criteria.items():
                if record.get(key.upper()) != value:
                    match = False
                    break
            if match:
                results.append(record)
        return results


class LegacyAdapter(Target):
    """Adapter for legacy system."""

    def __init__(self, legacy: LegacySystem):
        self._legacy = legacy

    def request(self, data: Any) -> Any:
        """Adapt modern request to legacy format."""
        if isinstance(data, dict):
            action = data.get("action", "")

            if action == "add":
                record = data.get("record", {})
                self._legacy.add_record(record)
                return {"success": True, "message": "Record added"}

            elif action == "find":
                criteria = data.get("criteria", {})
                results = self._legacy.find_records(criteria)
                return {
                    "success": True,
                    "results": results,
                    "count": len(results),
                }

        return {"success": False, "error": "Invalid request"}


class AdapterRegistry:
    """Registry for adapters."""

    def __init__(self):
        self._adapters: Dict[str, Target] = {}

    def register(self, name: str, adapter: Target) -> None:
        """Register an adapter."""
        self._adapters[name] = adapter

    def get(self, name: str) -> Optional[Target]:
        """Get an adapter."""
        return self._adapters.get(name)

    def list_adapters(self) -> List[str]:
        """List all adapters."""
        return list(self._adapters.keys())


class AdapterAction(BaseAction):
    """Adapter pattern action."""
    action_type = "adapter"
    display_name = "适配器模式"
    description = "接口适配转换"

    def __init__(self):
        super().__init__()
        self._registry = AdapterRegistry()
        self._adaptees: Dict[str, Any] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "adapt")

            if operation == "register":
                return self._register_adapter(params)
            elif operation == "adapt":
                return self._adapt(params)
            elif operation == "adapt_data":
                return self._adapt_data(params)
            elif operation == "adapt_api":
                return self._adapt_api(params)
            elif operation == "adapt_legacy":
                return self._adapt_legacy(params)
            elif operation == "list":
                return self._list_adapters()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Adapter error: {str(e)}")

    def _register_adapter(self, params: Dict[str, Any]) -> ActionResult:
        """Register an adapter."""
        name = params.get("name")
        adapter_type = params.get("type", "object")

        if not name:
            return ActionResult(success=False, message="name is required")

        if adapter_type == "object":
            adaptee = Adaptee()
            adapter = ObjectAdapter(adaptee)
            self._adaptees[name] = adaptee
        elif adapter_type == "data":
            adapter = DataAdapter()
        elif adapter_type == "api":
            client = APIClient()
            adapter = APIClientAdapter(client)
        elif adapter_type == "legacy":
            legacy = LegacySystem()
            adapter = LegacyAdapter(legacy)
            self._adaptees[name] = legacy
        else:
            return ActionResult(success=False, message=f"Unknown adapter type: {adapter_type}")

        self._registry.register(name, adapter)

        return ActionResult(success=True, message=f"Adapter registered: {name}")

    def _adapt(self, params: Dict[str, Any]) -> ActionResult:
        """Use an adapter."""
        name = params.get("name")
        data = params.get("data")

        if not name:
            return ActionResult(success=False, message="name is required")

        adapter = self._registry.get(name)
        if not adapter:
            return ActionResult(success=False, message=f"Adapter not found: {name}")

        result = adapter.request(data)

        return ActionResult(success=result.get("success", False), message="Adapted", data={"result": result})

    def _adapt_data(self, params: Dict[str, Any]) -> ActionResult:
        """Adapt data format."""
        content = params.get("content")
        input_format = params.get("input_format", "json")
        output_format = params.get("output_format", "dict")

        adapter = DataAdapter()

        def json_to_dict(data):
            if isinstance(data, str):
                import json
                return json.loads(data)
            return data

        def dict_to_json(data):
            if isinstance(data, dict):
                import json
                return json.dumps(data)
            return data

        adapter.register_adapter("json_to_dict", json_to_dict)
        adapter.register_adapter("dict_to_json", dict_to_json)

        result = adapter.request({"format": input_format, "content": content})

        return ActionResult(success=True, message="Data adapted", data={"result": result})

    def _adapt_api(self, params: Dict[str, Any]) -> ActionResult:
        """Adapt API client."""
        endpoint = params.get("endpoint", "/")
        method = params.get("method", "GET")
        body = params.get("body")

        client = APIClient()
        adapter = APIClientAdapter(client)

        result = adapter.request({
            "endpoint": endpoint,
            "method": method,
            "body": body,
        })

        return ActionResult(success=True, message="API adapted", data={"result": result})

    def _adapt_legacy(self, params: Dict[str, Any]) -> ActionResult:
        """Adapt legacy system."""
        action = params.get("action", "")
        record = params.get("record")
        criteria = params.get("criteria", {})

        legacy = LegacySystem()
        adapter = LegacyAdapter(legacy)

        data = {"action": action}
        if record:
            data["record"] = record
        if criteria:
            data["criteria"] = criteria

        result = adapter.request(data)

        return ActionResult(success=result.get("success", False), message="Legacy adapted", data={"result": result})

    def _list_adapters(self) -> ActionResult:
        """List all adapters."""
        adapters = self._registry.list_adapters()
        return ActionResult(success=True, message=f"{len(adapters)} adapters", data={"adapters": adapters})
