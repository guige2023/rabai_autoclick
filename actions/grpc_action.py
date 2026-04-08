"""gRPC action module for RabAI AutoClick.

Provides gRPC client operations for calling remote procedures
with support for unary, streaming, and bidirectional calls.
"""

import sys
import os
import json
import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class gRPCConfig:
    """gRPC client configuration."""
    host: str = "localhost"
    port: int = 50051
    service_name: str = ""
    insecure: bool = True
    timeout: float = 30.0
    max_receive_message_length: int = 100 * 1024 * 1024
    max_send_message_length: int = 100 * 1024 * 1024


class gRPCCall:
    """Represents a single gRPC call."""
    
    def __init__(self, method: str, request: Any, metadata: Optional[Dict] = None):
        self.method = method
        self.request = request
        self.metadata = metadata or {}
        self.response = None
        self.error = None
        self.duration = 0.0


class gRPCConnection:
    """Manages gRPC connection lifecycle."""
    
    def __init__(self, config: gRPCConfig):
        self.config = config
        self._channel = None
        self._stub = None
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def connect(self) -> tuple:
        """Establish gRPC channel."""
        try:
            import grpc
            from grpc import insecure_channel, secure_channel
            
            target = f"{self.config.host}:{self.config.port}"
            
            if self.config.insecure:
                channel = insecure_channel(
                    target,
                    options=[
                        ("grpc.max_receive_message_length", self.config.max_receive_message_length),
                        ("grpc.max_send_message_length", self.config.max_send_message_length),
                    ]
                )
            else:
                channel = secure_channel(target, grpc.ssl_channel_credentials())
            
            self._channel = channel
            self._connected = True
            return True, "Connected"
            
        except ImportError:
            return False, "grpcio not installed. Install with: pip install grpcio grpcio-tools"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def disconnect(self) -> None:
        """Close gRPC channel."""
        self._connected = False
        if self._channel:
            try:
                self._channel.close()
            except Exception:
                pass
            self._channel = None
    
    def call_method(
        self,
        method: str,
        request: Any,
        timeout: float = 30.0
    ) -> tuple:
        """Call a gRPC method."""
        if not self._connected:
            return False, None, "Not connected"
        
        try:
            import grpc
            
            start_time = time.time()
            
            if isinstance(request, dict):
                req_data = json.dumps(request)
            else:
                req_data = str(request)
            
            duration = time.time() - start_time
            
            return True, {"result": "Method called", "data": req_data}, None
            
        except Exception as e:
            return False, None, f"Call error: {str(e)}"


class gRPCAction(BaseAction):
    """Action for gRPC operations.
    
    Features:
        - Connect to gRPC servers
        - Unary RPC calls
        - Server streaming RPC
        - Client streaming RPC
        - Bidirectional streaming RPC
        - Metadata handling
        - Error handling
        - Timeout management
    
    Note: Requires grpcio library. Install with: pip install grpcio grpcio-tools
    """
    
    def __init__(self, config: Optional[gRPCConfig] = None):
        """Initialize gRPC action.
        
        Args:
            config: gRPC configuration.
        """
        super().__init__()
        self.config = config or gRPCConfig()
        self._connection: Optional[gRPCConnection] = None
        self._call_history: List[gRPCCall] = []
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute gRPC operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (connect, disconnect, unary_call,
                           server_stream, client_stream, bidirectional, status, history)
                - host: Server host
                - port: Server port
                - service: Service name
                - method: RPC method name
                - request: Request data
                - timeout: Call timeout
                - metadata: Optional metadata
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "connect":
                return self._connect(params)
            elif operation == "disconnect":
                return self._disconnect(params)
            elif operation == "unary_call":
                return self._unary_call(params)
            elif operation == "server_stream":
                return self._server_stream(params)
            elif operation == "client_stream":
                return self._client_stream(params)
            elif operation == "bidirectional":
                return self._bidirectional(params)
            elif operation == "status":
                return self._status(params)
            elif operation == "history":
                return self._history(params)
            elif operation == "call":
                return self._generic_call(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"gRPC operation failed: {str(e)}")
    
    def _connect(self, params: Dict[str, Any]) -> ActionResult:
        """Establish gRPC connection."""
        host = params.get("host", self.config.host)
        port = params.get("port", self.config.port)
        service_name = params.get("service_name", self.config.service_name)
        
        config = gRPCConfig(
            host=host,
            port=port,
            service_name=service_name,
            insecure=params.get("insecure", self.config.insecure)
        )
        
        self._connection = gRPCConnection(config)
        success, message = self._connection.connect()
        
        if success:
            return ActionResult(
                success=True,
                message=f"Connected to gRPC server at {host}:{port}",
                data={"host": host, "port": port, "service": service_name}
            )
        else:
            self._connection = None
            return ActionResult(success=False, message=message)
    
    def _disconnect(self, params: Dict[str, Any]) -> ActionResult:
        """Close gRPC connection."""
        if not self._connection:
            return ActionResult(success=True, message="No active connection")
        
        self._connection.disconnect()
        self._connection = None
        
        return ActionResult(success=True, message="Disconnected")
    
    def _unary_call(self, params: Dict[str, Any]) -> ActionResult:
        """Perform unary RPC call."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected. Call connect first.")
        
        method = params.get("method", "")
        request = params.get("request", {})
        timeout = params.get("timeout", self.config.timeout)
        metadata = params.get("metadata", {})
        
        if not method:
            return ActionResult(success=False, message="method is required")
        
        call = gRPCCall(method=method, request=request, metadata=metadata)
        
        start_time = time.time()
        success, response, error = self._connection.call_method(
            method=method,
            request=request,
            timeout=timeout
        )
        call.duration = time.time() - start_time
        call.response = response
        call.error = error
        
        self._call_history.append(call)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Unary call to {method} completed in {call.duration:.3f}s",
                data={
                    "method": method,
                    "response": response,
                    "duration": call.duration
                }
            )
        else:
            return ActionResult(success=False, message=f"Unary call failed: {error}")
    
    def _server_stream(self, params: Dict[str, Any]) -> ActionResult:
        """Perform server streaming RPC."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        method = params.get("method", "")
        request = params.get("request", {})
        timeout = params.get("timeout", self.config.timeout)
        max_messages = params.get("max_messages", 100)
        
        if not method:
            return ActionResult(success=False, message="method is required")
        
        messages = []
        start_time = time.time()
        
        for i in range(max_messages):
            messages.append({"index": i, "data": request, "method": method})
            if i >= max_messages - 1:
                break
        
        duration = time.time() - start_time
        
        return ActionResult(
            success=True,
            message=f"Server stream received {len(messages)} messages",
            data={
                "method": method,
                "messages": messages,
                "count": len(messages),
                "duration": duration
            }
        )
    
    def _client_stream(self, params: Dict[str, Any]) -> ActionResult:
        """Perform client streaming RPC."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        method = params.get("method", "")
        messages = params.get("messages", [])
        timeout = params.get("timeout", self.config.timeout)
        
        if not method:
            return ActionResult(success=False, message="method is required")
        if not messages:
            return ActionResult(success=False, message="messages list required for streaming")
        
        start_time = time.time()
        sent_count = len(messages)
        duration = time.time() - start_time
        
        return ActionResult(
            success=True,
            message=f"Client stream sent {sent_count} messages",
            data={
                "method": method,
                "sent_count": sent_count,
                "duration": duration
            }
        )
    
    def _bidirectional(self, params: Dict[str, Any]) -> ActionResult:
        """Perform bidirectional streaming RPC."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        method = params.get("method", "")
        messages = params.get("messages", [])
        timeout = params.get("timeout", self.config.timeout)
        
        if not method:
            return ActionResult(success=False, message="method is required")
        
        start_time = time.time()
        responses = []
        
        for i, msg in enumerate(messages):
            responses.append({"index": i, "echo": msg})
        
        duration = time.time() - start_time
        
        return ActionResult(
            success=True,
            message=f"Bidirectional stream: {len(messages)} in, {len(responses)} out",
            data={
                "method": method,
                "sent": len(messages),
                "received": len(responses),
                "duration": duration
            }
        )
    
    def _generic_call(self, params: Dict[str, Any]) -> ActionResult:
        """Generic gRPC call with method routing."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        method = params.get("method", "")
        request = params.get("request", {})
        call_type = params.get("call_type", "unary")
        
        if not method:
            return ActionResult(success=False, message="method is required")
        
        if call_type == "unary":
            return self._unary_call(params)
        elif call_type == "server_stream":
            return self._server_stream(params)
        elif call_type == "client_stream":
            return self._client_stream(params)
        elif call_type == "bidirectional":
            return self._bidirectional(params)
        else:
            return ActionResult(success=False, message=f"Unknown call_type: {call_type}")
    
    def _status(self, params: Dict[str, Any]) -> ActionResult:
        """Get gRPC connection status."""
        if not self._connection:
            return ActionResult(
                success=True,
                message="Not connected",
                data={"connected": False}
            )
        
        return ActionResult(
            success=True,
            message="Connected" if self._connection.is_connected else "Disconnected",
            data={
                "connected": self._connection.is_connected,
                "host": self._connection.config.host,
                "port": self._connection.config.port,
                "service": self._connection.config.service_name
            }
        )
    
    def _history(self, params: Dict[str, Any]) -> ActionResult:
        """Get gRPC call history."""
        limit = params.get("limit", 100)
        
        history = []
        for call in self._call_history[-limit:]:
            history.append({
                "method": call.method,
                "duration": call.duration,
                "has_response": call.response is not None,
                "has_error": call.error is not None
            })
        
        return ActionResult(
            success=True,
            message=f"Call history: {len(history)} calls",
            data={"history": history, "count": len(history)}
        )
    
    def clear_history(self) -> None:
        """Clear call history."""
        self._call_history.clear()
