"""
API adapter pattern module.

Provides adapter implementations for integrating with various
external API styles and protocols.

Author: Aito Auto Agent
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Optional,
    TypeVar,
    Generic,
)


T = TypeVar('T')
R = TypeVar('R')


class ApiAdapter(ABC, Generic[T, R]):
    """
    Abstract base class for API adapters.

    Adapters translate between different API formats and protocols.
    """

    @abstractmethod
    def request(self, endpoint: str, **kwargs) -> R:
        """Make a request through the adapter."""
        pass

    @abstractmethod
    def response(self, raw_response: T) -> R:
        """Process raw response into standard format."""
        pass


@dataclass
class RequestConfig:
    """Configuration for API requests."""
    url: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    body: Optional[Any] = None
    timeout: float = 30.0
    retries: int = 0


@dataclass
class ResponseWrapper:
    """Standardized response wrapper."""
    status_code: int
    headers: dict[str, str]
    body: Any
    raw: Any = None


class RestApiAdapter(ApiAdapter[dict, ResponseWrapper]):
    """
    REST API adapter with standardized interface.

    Example:
        adapter = RestApiAdapter(base_url="https://api.example.com")
        response = adapter.request("/users/123", method="GET")
    """

    def __init__(
        self,
        base_url: str = "",
        default_headers: Optional[dict[str, str]] = None,
        timeout: float = 30.0
    ):
        self._base_url = base_url.rstrip("/")
        self._default_headers = default_headers or {}
        self._timeout = timeout

    def request(self, endpoint: str, **kwargs) -> ResponseWrapper:
        """Make REST API request."""
        import requests

        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        headers = {**self._default_headers, **kwargs.get("headers", {})}

        response = requests.request(
            url=url,
            method=kwargs.get("method", "GET"),
            headers=headers,
            params=kwargs.get("params"),
            json=kwargs.get("body"),
            timeout=kwargs.get("timeout", self._timeout)
        )

        return ResponseWrapper(
            status_code=response.status_code,
            headers=dict(response.headers),
            body=response.json() if response.content else None,
            raw=response
        )

    def response(self, raw_response: dict) -> ResponseWrapper:
        """Process REST response."""
        return ResponseWrapper(
            status_code=raw_response.get("status_code", 0),
            headers=raw_response.get("headers", {}),
            body=raw_response.get("body")
        )


class GraphQLAdapter(ApiAdapter[dict, ResponseWrapper]):
    """
    GraphQL API adapter.

    Example:
        adapter = GraphQLAdapter(endpoint="https://api.example.com/graphql")
        response = adapter.request(
            query="{ user(id: 123) { name email } }"
        )
    """

    def __init__(
        self,
        endpoint: str,
        headers: Optional[dict[str, str]] = None
    ):
        self._endpoint = endpoint
        self._headers = headers or {}

    def request(
        self,
        query: str,
        variables: Optional[dict] = None,
        operation_name: Optional[str] = None,
        **kwargs
    ) -> ResponseWrapper:
        """Execute GraphQL query/mutation."""
        import requests

        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        if operation_name:
            payload["operationName"] = operation_name

        response = requests.post(
            self._endpoint,
            json=payload,
            headers=self._headers,
            timeout=kwargs.get("timeout", 30.0)
        )

        result = response.json()

        errors = result.get("errors", [])
        status_code = 400 if errors else 200

        return ResponseWrapper(
            status_code=response.status_code or status_code,
            headers=dict(response.headers),
            body=result.get("data"),
            raw=result
        )

    def query(self, query: str, variables: Optional[dict] = None) -> ResponseWrapper:
        """Execute a GraphQL query."""
        return self.request(query=query, variables=variables)

    def mutation(
        self,
        mutation: str,
        variables: Optional[dict] = None
    ) -> ResponseWrapper:
        """Execute a GraphQL mutation."""
        return self.request(query=mutation, variables=variables)

    def response(self, raw_response: dict) -> ResponseWrapper:
        """Process GraphQL response."""
        return ResponseWrapper(
            status_code=200 if raw_response.get("data") else 400,
            headers={},
            body=raw_response.get("data"),
            raw=raw_response
        )


class WebSocketAdapter(ApiAdapter[Any, Any]):
    """
    WebSocket API adapter for real-time communication.

    Example:
        adapter = WebSocketAdapter("wss://api.example.com/ws")
        adapter.connect()
        adapter.send({"type": "subscribe", "channel": "prices"})
    """

    def __init__(
        self,
        url: str,
        headers: Optional[dict[str, str]] = None
    ):
        self._url = url
        self._headers = headers or {}
        self._ws = None

    def request(self, endpoint: str = "", **kwargs) -> Any:
        """Send message over WebSocket."""
        if self._ws is None:
            self.connect()

        message = kwargs.get("message")
        if message:
            self._ws.send(message)

        return self._receive()

    def connect(self) -> None:
        """Establish WebSocket connection."""
        import websocket

        self._ws = websocket.WebSocketApp(
            self._url,
            header=self._headers,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )

    def disconnect(self) -> None:
        """Close WebSocket connection."""
        if self._ws:
            self._ws.close()
            self._ws = None

    def send(self, data: Any) -> None:
        """Send data over WebSocket."""
        if self._ws:
            import json
            self._ws.send(json.dumps(data))

    def _receive(self) -> Any:
        """Receive message from WebSocket."""
        pass

    def _on_message(self, ws: Any, message: str) -> None:
        """Handle incoming message."""
        pass

    def _on_error(self, ws: Any, error: Exception) -> None:
        """Handle WebSocket error."""
        pass

    def _on_close(self, ws: Any) -> None:
        """Handle WebSocket close."""
        pass

    def response(self, raw_response: Any) -> Any:
        """Process WebSocket message."""
        return raw_response


class BatchAdapter(ApiAdapter[list, list]):
    """
    Batch API adapter for request batching.

    Example:
        adapter = BatchAdapter(RestApiAdapter(base_url="https://api.example.com"))
        results = adapter.request([
            {"endpoint": "/users/1"},
            {"endpoint": "/users/2"}
        ])
    """

    def __init__(
        self,
        wrapped: ApiAdapter,
        batch_size: int = 10
    ):
        self._wrapped = wrapped
        self._batch_size = batch_size

    def request(self, requests: list[dict], **kwargs) -> list[ResponseWrapper]:
        """Execute batch of requests."""
        results = []
        for req in requests:
            try:
                result = self._wrapped.request(
                    endpoint=req.get("endpoint", ""),
                    method=req.get("method", "GET"),
                    **req
                )
                results.append(result)
            except Exception as e:
                results.append(ResponseWrapper(
                    status_code=0,
                    headers={},
                    body={"error": str(e)}
                ))
        return results

    def response(self, raw_response: list) -> list:
        """Process batch responses."""
        return [self._wrapped.response(r) if hasattr(r, '__dict__') else r for r in raw_response]


class TransformAdapter(ApiAdapter[T, R]):
    """
    Adapter with request/response transformation.

    Example:
        adapter = TransformAdapter(
            wrapped=RestApiAdapter(base_url="https://api.example.com"),
            request_transform=lambda req: {"body": {"data": req}},
            response_transform=lambda res: res.body.get("result")
        )
    """

    def __init__(
        self,
        wrapped: ApiAdapter,
        request_transform: Optional[Callable[[dict], dict]] = None,
        response_transform: Optional[Callable[[ResponseWrapper], Any]] = None
    ):
        self._wrapped = wrapped
        self._request_transform = request_transform or (lambda x: x)
        self._response_transform = response_transform or (lambda x: x)

    def request(self, endpoint: str, **kwargs) -> R:
        """Make request with transformation."""
        transformed = self._request_transform({
            "endpoint": endpoint,
            **kwargs
        })
        result = self._wrapped.request(**transformed)
        return self._response_transform(result)

    def response(self, raw_response: T) -> R:
        """Process response with transformation."""
        return self._response_transform(raw_response)


def create_rest_adapter(
    base_url: str,
    **kwargs
) -> RestApiAdapter:
    """Factory for REST API adapter."""
    return RestApiAdapter(base_url=base_url, **kwargs)


def create_graphql_adapter(
    endpoint: str,
    **kwargs
) -> GraphQLAdapter:
    """Factory for GraphQL adapter."""
    return GraphQLAdapter(endpoint=endpoint, **kwargs)


def create_websocket_adapter(
    url: str,
    **kwargs
) -> WebSocketAdapter:
    """Factory for WebSocket adapter."""
    return WebSocketAdapter(url=url, **kwargs)
