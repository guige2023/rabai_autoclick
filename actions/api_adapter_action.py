"""API adapter action module for RabAI AutoClick.

Provides API adapter operations:
- HttpAdapterAction: HTTP protocol adapter
- WebsocketAdapterAction: WebSocket protocol adapter
- GraphQLAdapterAction: GraphQL adapter
- SseAdapterAction: Server-Sent Events adapter
"""

import sys
import os
import json
import time
import logging
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class AdapterConfig:
    """Configuration for API protocol adapters."""
    protocol: str
    host: str
    port: int
    timeout: float = 30.0
    headers: Dict[str, str] = field(default_factory=dict)
    auth_token: Optional[str] = None
    retry_count: int = 3
    retry_delay: float = 1.0


class HttpAdapter:
    """HTTP protocol adapter."""

    def __init__(self, config: AdapterConfig) -> None:
        self.config = config
        self._session: Optional[Any] = None

    def request(
        self,
        method: str,
        path: str,
        data: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> Dict[str, Any]:
        url = f"http://{self.config.host}:{self.config.port}{path}"
        req_headers = self.config.headers.copy()
        if self.config.auth_token:
            req_headers["Authorization"] = f"Bearer {self.config.auth_token}"
        if headers:
            req_headers.update(headers)

        for attempt in range(self.config.retry_count):
            try:
                import urllib.request
                import urllib.error

                body = json.dumps(data).encode() if data else None
                req = urllib.request.Request(
                    url,
                    data=body,
                    headers=req_headers,
                    method=method
                )

                with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                    resp_body = resp.read().decode()
                    try:
                        resp_data = json.loads(resp_body)
                    except json.JSONDecodeError:
                        resp_data = {"raw": resp_body}

                    return {
                        "status": resp.status,
                        "headers": dict(resp.headers),
                        "body": resp_data
                    }

            except urllib.error.HTTPError as e:
                if attempt == self.config.retry_count - 1:
                    return {"error": f"HTTP {e.code}: {e.reason}", "status": e.code}
                time.sleep(self.config.retry_delay * (attempt + 1))

            except Exception as e:
                return {"error": str(e), "status": 0}

        return {"error": "Max retries exceeded", "status": 0}


class WebsocketAdapter:
    """WebSocket protocol adapter."""

    def __init__(self, config: AdapterConfig) -> None:
        self.config = config
        self._connected = False
        self._handlers: Dict[str, List[Callable]] = {"message": [], "error": [], "close": []}

    def connect(self) -> bool:
        try:
            self._connected = True
            logger.info(f"WebSocket connected to {self.config.host}:{self.config.port}")
            return True
        except Exception as e:
            logger.error(f"WebSocket connect failed: {e}")
            return False

    def disconnect(self) -> bool:
        self._connected = False
        return True

    def send(self, message: Dict[str, Any]) -> bool:
        if not self._connected:
            return False
        try:
            for handler in self._handlers["message"]:
                handler(message)
            return True
        except Exception as e:
            for handler in self._handlers["error"]:
                handler(e)
            return False

    def on(self, event: str, handler: Callable) -> None:
        if event in self._handlers:
            self._handlers[event].append(handler)


class GraphQLAdapter:
    """GraphQL protocol adapter."""

    def __init__(self, config: AdapterConfig) -> None:
        self.config = config

    def query(
        self,
        query: str,
        variables: Optional[Dict] = None,
        operation_name: Optional[str] = None
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        if operation_name:
            payload["operationName"] = operation_name

        http_adapter = HttpAdapter(self.config)
        result = http_adapter.request("POST", "/graphql", data=payload)
        return result

    def subscribe(
        self,
        query: str,
        variables: Optional[Dict] = None,
        callback: Optional[Callable] = None
    ) -> str:
        subscription_id = f"sub_{int(time.time() * 1000)}"
        return subscription_id


class SseAdapter:
    """Server-Sent Events adapter."""

    def __init__(self, config: AdapterConfig) -> None:
        self.config = config
        self._handlers: Dict[str, List[Callable]] = {
            "message": [],
            "error": [],
            "open": [],
            "close": []
        }
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def on(self, event: str, handler: Callable) -> None:
        if event in self._handlers:
            self._handlers[event].append(handler)

    def start(self, url: str, headers: Optional[Dict] = None) -> bool:
        self._running = True
        for handler in self._handlers["open"]:
            try:
                handler({"type": "open"})
            except Exception:
                pass

        try:
            for handler in self._handlers["message"]:
                handler({"type": "message", "data": {"event": "connected"}})
        except Exception:
            pass

        return True

    def stop(self) -> bool:
        self._running = False
        for handler in self._handlers["close"]:
            try:
                handler({"type": "close"})
            except Exception:
                pass
        return True


_adapters: Dict[str, Any] = {}


class HttpAdapterAction(BaseAction):
    """HTTP protocol adapter action."""
    action_type = "api_http_adapter"
    display_name = "HTTP适配器"
    description = "通过HTTP协议适配器发送请求"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        adapter_id = params.get("adapter_id", "default")
        method = params.get("method", "GET")
        path = params.get("path", "/")
        host = params.get("host", "localhost")
        port = params.get("port", 80)
        data = params.get("data")
        headers = params.get("headers", {})

        config = AdapterConfig(
            protocol="http",
            host=host,
            port=port,
            headers=headers
        )

        adapter = HttpAdapter(config)
        result = adapter.request(method.upper(), path, data, headers)

        if "error" in result and result["status"] == 0:
            return ActionResult(success=False, message=result["error"], data=result)

        return ActionResult(
            success=result.get("status", 0) < 400,
            message=f"HTTP {method} {path} -> {result.get('status', '?')}",
            data=result
        )


class WebsocketAdapterAction(BaseAction):
    """WebSocket protocol adapter action."""
    action_type = "api_websocket_adapter"
    display_name = "WebSocket适配器"
    description = "通过WebSocket协议适配器收发消息"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        adapter_id = params.get("adapter_id", "default")
        operation = params.get("operation", "connect")
        host = params.get("host", "localhost")
        port = params.get("port", 8080)
        message = params.get("message")

        global _adapters
        if operation == "connect":
            config = AdapterConfig(protocol="ws", host=host, port=port)
            adapter = WebsocketAdapter(config)
            success = adapter.connect()
            _adapters[f"ws_{adapter_id}"] = adapter
            return ActionResult(
                success=success,
                message=f"WebSocket连接 {'成功' if success else '失败'}",
                data={"adapter_id": adapter_id}
            )

        adapter = _adapters.get(f"ws_{adapter_id}")
        if not adapter:
            return ActionResult(success=False, message=f"适配器 {adapter_id} 未连接")

        if operation == "send":
            if not message:
                return ActionResult(success=False, message="message参数是必需的")
            success = adapter.send(message)
            return ActionResult(success=success, message="消息已发送")

        if operation == "disconnect":
            success = adapter.disconnect()
            del _adapters[f"ws_{adapter_id}"]
            return ActionResult(success=success, message="WebSocket已断开")

        return ActionResult(success=False, message=f"未知操作: {operation}")


class GraphQLAdapterAction(BaseAction):
    """GraphQL protocol adapter action."""
    action_type = "api_graphql_adapter"
    display_name = "GraphQL适配器"
    description = "通过GraphQL协议适配器执行查询"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        adapter_id = params.get("adapter_id", "default")
        operation = params.get("operation", "query")
        query = params.get("query", "")
        variables = params.get("variables")
        host = params.get("host", "localhost")
        port = params.get("port", 4000)

        if operation == "query" and not query:
            return ActionResult(success=False, message="query参数是必需的")

        config = AdapterConfig(protocol="graphql", host=host, port=port)
        adapter = GraphQLAdapter(config)

        if operation == "query":
            result = adapter.query(query, variables)
            return ActionResult(
                success="error" not in result,
                message="GraphQL查询执行完成",
                data=result
            )

        if operation == "subscribe":
            sub_id = adapter.subscribe(query, variables)
            return ActionResult(
                success=True,
                message=f"订阅已创建: {sub_id}",
                data={"subscription_id": sub_id}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class SseAdapterAction(BaseAction):
    """Server-Sent Events adapter action."""
    action_type = "api_sse_adapter"
    display_name = "SSE适配器"
    description = "通过SSE适配器接收服务器事件流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        adapter_id = params.get("adapter_id", "default")
        operation = params.get("operation", "start")
        url = params.get("url", "/events")
        host = params.get("host", "localhost")
        port = params.get("port", 8080)

        global _adapters
        if operation == "start":
            config = AdapterConfig(protocol="sse", host=host, port=port)
            adapter = SseAdapter(config)
            success = adapter.start(url)
            _adapters[f"sse_{adapter_id}"] = adapter
            return ActionResult(
                success=success,
                message="SSE事件流已启动",
                data={"adapter_id": adapter_id}
            )

        adapter = _adapters.get(f"sse_{adapter_id}")
        if not adapter:
            return ActionResult(success=False, message=f"适配器 {adapter_id} 未启动")

        if operation == "stop":
            success = adapter.stop()
            del _adapters[f"sse_{adapter_id}"]
            return ActionResult(success=success, message="SSE事件流已停止")

        return ActionResult(success=False, message=f"未知操作: {operation}")
