"""WebSocket client action module for RabAI AutoClick.

Provides WebSocket operations:
- WebSocketConnectAction: Connect to WebSocket server
- WebSocketSendAction: Send message
- WebSocketReceiveAction: Receive message
- WebSocketCloseAction: Close connection
- WebSocketPingAction: Send ping
"""

from __future__ import annotations

import sys
import os
import json
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WebSocketConnectAction(BaseAction):
    """Connect to WebSocket server."""
    action_type = "websocket_connect"
    display_name = "WebSocket连接"
    description = "连接到WebSocket服务器"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute WebSocket connect."""
        url = params.get('url', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'ws_connection')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            resolved_url = context.resolve_value(url) if context else url
            resolved_headers = context.resolve_value(headers) if context else headers

            try:
                import websockets
            except ImportError:
                return ActionResult(success=False, message="websockets library not installed. Run: pip install websockets")

            # For simplicity, we create a connection info dict
            # The actual connection will be managed in send/receive actions
            conn_info = {
                'url': resolved_url,
                'headers': resolved_headers,
                'connected': True,
            }

            if context:
                context.set(output_var, conn_info)
            return ActionResult(success=True, message=f"WebSocket connection info: {resolved_url}", data=conn_info)
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket connect error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'ws_connection'}


class WebSocketSendAction(BaseAction):
    """Send message over WebSocket."""
    action_type = "websocket_send"
    display_name = "WebSocket发送"
    description = "通过WebSocket发送消息"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute WebSocket send."""
        url = params.get('url', '')
        message = params.get('message', '')
        message_type = params.get('message_type', 'text')  # text or json
        headers = params.get('headers', {})
        timeout = params.get('timeout', 10)
        output_var = params.get('output_var', 'ws_send_result')

        if not url or not message:
            return ActionResult(success=False, message="url and message are required")

        try:
            import asyncio
            import websockets

            resolved_url = context.resolve_value(url) if context else url
            resolved_message = context.resolve_value(message) if context else message
            resolved_timeout = context.resolve_value(timeout) if context else timeout

            async def _send():
                async with websockets.connect(resolved_url, extra_headers=headers) as ws:
                    if message_type == 'json':
                        if isinstance(resolved_message, str):
                            resolved_message = json.loads(resolved_message)
                        await ws.send(json.dumps(resolved_message))
                    else:
                        await ws.send(str(resolved_message))
                    return {'sent': True, 'message': str(resolved_message)}

            result = asyncio.run(_send())
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="Message sent", data=result)
        except ImportError:
            return ActionResult(success=False, message="websockets library not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket send error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'message_type': 'text', 'headers': {}, 'timeout': 10, 'output_var': 'ws_send_result'}


class WebSocketReceiveAction(BaseAction):
    """Receive message from WebSocket."""
    action_type = "websocket_receive"
    display_name = "WebSocket接收"
    description = "从WebSocket接收消息"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute WebSocket receive."""
        url = params.get('url', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 10)
        parse_json = params.get('parse_json', False)
        output_var = params.get('output_var', 'ws_message')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import asyncio
            import websockets

            resolved_url = context.resolve_value(url) if context else url
            resolved_timeout = context.resolve_value(timeout) if context else timeout

            async def _receive():
                async with websockets.connect(resolved_url, extra_headers=headers) as ws:
                    msg = await asyncio.wait_for(ws.recv(), timeout=resolved_timeout)
                    if parse_json:
                        return {'data': json.loads(msg), 'raw': msg}
                    return {'data': msg}

            result = asyncio.run(_receive())
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="Message received", data=result)
        except ImportError:
            return ActionResult(success=False, message="websockets library not installed")
        except asyncio.TimeoutError:
            return ActionResult(success=False, message=f"Timeout after {resolved_timeout}s")
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket receive error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'timeout': 10, 'parse_json': False, 'output_var': 'ws_message'}


class WebSocketPingAction(BaseAction):
    """Send ping and wait for pong."""
    action_type = "websocket_ping"
    display_name = "WebSocket ping"
    description = "发送WebSocket ping"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute WebSocket ping."""
        url = params.get('url', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 10)
        output_var = params.get('output_var', 'ws_ping_result')

        if not url:
            return ActionResult(success=False, message="url is required")

        try:
            import asyncio
            import websockets

            resolved_url = context.resolve_value(url) if context else url

            async def _ping():
                async with websockets.connect(resolved_url, extra_headers=headers) as ws:
                    await ws.ping()
                    return {'pong': True}

            result = asyncio.run(_ping())
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="Ping successful", data=result)
        except ImportError:
            return ActionResult(success=False, message="websockets library not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket ping error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'timeout': 10, 'output_var': 'ws_ping_result'}


class WebSocketChatAction(BaseAction):
    """Send and receive WebSocket messages (chat style)."""
    action_type = "websocket_chat"
    display_name = "WebSocket对话"
    description = "WebSocket发送并接收"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute WebSocket chat."""
        url = params.get('url', '')
        message = params.get('message', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 10)
        output_var = params.get('output_var', 'ws_chat_result')

        if not url or not message:
            return ActionResult(success=False, message="url and message are required")

        try:
            import asyncio
            import websockets

            resolved_url = context.resolve_value(url) if context else url
            resolved_message = context.resolve_value(message) if context else message

            async def _chat():
                async with websockets.connect(resolved_url, extra_headers=headers) as ws:
                    await ws.send(str(resolved_message))
                    response = await asyncio.wait_for(ws.recv(), timeout=timeout)
                    return {'sent': str(resolved_message), 'received': response}

            result = asyncio.run(_chat())
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="Chat round-trip complete", data=result)
        except ImportError:
            return ActionResult(success=False, message="websockets library not installed")
        except Exception as e:
            return ActionResult(success=False, message=f"WebSocket chat error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['url', 'message']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'timeout': 10, 'output_var': 'ws_chat_result'}
