"""
RPC Action Module.

Provides generic RPC capabilities for distributed system communication.
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import time
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class RPCType(Enum):
    """RPC call types."""
    SYNC = "sync"
    ASYNC = "async"
    BROADCAST = "broadcast"
    STREAM = "stream"


@dataclass
class RPCRequest:
    """RPC request structure."""
    method: str
    params: Dict[str, Any]
    rpc_type: RPCType = RPCType.SYNC
    request_id: Optional[str] = None
    timeout: float = 30.0
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class RPCResponse:
    """RPC response structure."""
    request_id: str
    result: Any
    error: Optional[str] = None
    elapsed_ms: float = 0.0


@dataclass
class RPCEndpoint:
    """RPC endpoint registration."""
    name: str
    handler: Callable
    param_types: List[type] = field(default_factory=list)
    return_type: Optional[type] = None
    description: str = ""


class RPCAction:
    """
    RPC action handler.
    
    Provides RPC server and client capabilities for distributed calls.
    
    Example:
        rpc = RPCAction()
        rpc.register("add", lambda a, b: a + b)
        response = rpc.call("add", {"a": 1, "b": 2})
    """
    
    def __init__(self):
        """Initialize RPC handler."""
        self._endpoints: Dict[str, RPCEndpoint] = {}
        self._pending_requests: Dict[str, RPCRequest] = {}
        self._response_handlers: Dict[str, Callable] = {}
    
    def register(
        self,
        name: str,
        handler: Callable,
        description: str = ""
    ) -> None:
        """
        Register an RPC endpoint.
        
        Args:
            name: Method name
            handler: Handler function
            description: Optional description
        """
        self._endpoints[name] = RPCEndpoint(
            name=name,
            handler=handler,
            description=description
        )
        logger.info(f"Registered RPC endpoint: {name}")
    
    def unregister(self, name: str) -> bool:
        """
        Unregister an RPC endpoint.
        
        Args:
            name: Method name
            
        Returns:
            True if unregistered
        """
        if name in self._endpoints:
            del self._endpoints[name]
            logger.info(f"Unregistered RPC endpoint: {name}")
            return True
        return False
    
    def list_endpoints(self) -> List[Dict[str, str]]:
        """
        List all registered endpoints.
        
        Returns:
            List of endpoint metadata
        """
        return [
            {
                "name": ep.name,
                "description": ep.description
            }
            for ep in self._endpoints.values()
        ]
    
    def call(
        self,
        method: str,
        params: Dict[str, Any],
        rpc_type: RPCType = RPCType.SYNC,
        request_id: Optional[str] = None,
        timeout: float = 30.0
    ) -> Union[RPCResponse, None]:
        """
        Make an RPC call.
        
        Args:
            method: Method name
            params: Method parameters
            rpc_type: Type of RPC call
            request_id: Optional request ID
            timeout: Call timeout
            
        Returns:
            RPCResponse for sync calls, None for async
        """
        request_id = request_id or self._generate_id()
        
        request = RPCRequest(
            method=method,
            params=params,
            rpc_type=rpc_type,
            request_id=request_id,
            timeout=timeout
        )
        
        if rpc_type == RPCType.ASYNC:
            return self._handle_async(request)
        
        if rpc_type == RPCType.BROADCAST:
            return self._handle_broadcast(request)
        
        return self._handle_sync(request)
    
    def _handle_sync(self, request: RPCRequest) -> RPCResponse:
        """Handle synchronous RPC call."""
        start = time.time()
        
        endpoint = self._endpoints.get(request.method)
        if not endpoint:
            return RPCResponse(
                request_id=request.request_id,
                result=None,
                error=f"Method not found: {request.method}",
                elapsed_ms=(time.time() - start) * 1000
            )
        
        try:
            result = endpoint.handler(**request.params)
            return RPCResponse(
                request_id=request.request_id,
                result=result,
                elapsed_ms=(time.time() - start) * 1000
            )
        except Exception as e:
            return RPCResponse(
                request_id=request.request_id,
                result=None,
                error=str(e),
                elapsed_ms=(time.time() - start) * 1000
            )
    
    def _handle_async(self, request: RPCRequest) -> None:
        """Handle asynchronous RPC call."""
        self._pending_requests[request.request_id] = request
        logger.debug(f"Async RPC queued: {request.request_id}")
    
    def _handle_broadcast(self, request: RPCRequest) -> RPCResponse:
        """Handle broadcast RPC call."""
        return RPCResponse(
            request_id=request.request_id,
            result={"broadcast_sent": True},
            elapsed_ms=0.0
        )
    
    def handle_response(
        self,
        request_id: str,
        result: Any,
        error: Optional[str] = None
    ) -> None:
        """
        Handle an RPC response.
        
        Args:
            request_id: Original request ID
            result: Response result
            error: Optional error message
        """
        if request_id in self._response_handlers:
            handler = self._response_handlers.pop(request_id)
            handler(result, error)
    
    def _generate_id(self) -> str:
        """Generate unique request ID."""
        return hashlib.sha256(
            f"{time.time()}{id(self)}".encode()
        ).hexdigest()[:16]
    
    def batch_call(
        self,
        calls: List[Dict[str, Any]]
    ) -> List[RPCResponse]:
        """
        Make multiple RPC calls in batch.
        
        Args:
            calls: List of call specifications
            
        Returns:
            List of responses
        """
        responses = []
        for call in calls:
            response = self.call(
                method=call.get("method", ""),
                params=call.get("params", {}),
                rpc_type=RPCType(call.get("rpc_type", "sync"))
            )
            responses.append(response)
        return responses
    
    def stream_call(
        self,
        method: str,
        params: Dict[str, Any],
        callback: Callable[[Any], None]
    ) -> str:
        """
        Start a streaming RPC call.
        
        Args:
            method: Method name
            params: Method parameters
            callback: Function to call for each response
            
        Returns:
            Stream ID
        """
        stream_id = self._generate_id()
        self._response_handlers[stream_id] = callback
        logger.debug(f"Streaming RPC started: {stream_id}")
        return stream_id
    
    def get_endpoint_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an endpoint.
        
        Args:
            name: Endpoint name
            
        Returns:
            Endpoint metadata or None
        """
        endpoint = self._endpoints.get(name)
        if not endpoint:
            return None
        
        return {
            "name": endpoint.name,
            "description": endpoint.description,
            "has_handler": endpoint.handler is not None
        }
