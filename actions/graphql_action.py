"""GraphQL client action for API queries and mutations.

This module provides a comprehensive GraphQL client implementation:
- Query and mutation execution
- Schema introspection
- Subscription support via WebSocket
- Batch operations
- Error handling and retries
- Caching and deduplication

Author: rabai_autoclick
Version: 1.0.0
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import urlparse

try:
    import strawberry
    STRAWBERRY_AVAILABLE = True
except ImportError:
    STRAWBERRY_AVAILABLE = False

try:
    import httpx
    HTTX_AVAILABLE = True
except ImportError:
    HTTX_AVAILABLE = False

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

logger = logging.getLogger(__name__)


class OperationType(Enum):
    """GraphQL operation types."""
    QUERY = "query"
    MUTATION = "mutation"
    SUBSCRIPTION = "subscription"


class ErrorClassification(Enum):
    """GraphQL error classification."""
    TRANSPORT = "transport"
    GRAPHQL = "graphql"
    VALIDATION = "validation"
    AUTHORIZATION = "authorization"
    UNKNOWN = "unknown"


@dataclass
class GraphQLError:
    """GraphQL error information."""
    message: str
    path: Optional[List[Union[str, int]]] = None
    locations: Optional[List[Dict[str, int]]] = None
    extensions: Optional[Dict[str, Any]] = None
    classification: ErrorClassification = ErrorClassification.UNKNOWN


@dataclass
class GraphQLResponse:
    """GraphQL response wrapper."""
    data: Optional[Dict[str, Any]] = None
    errors: List[GraphQLError] = field(default_factory=list)
    extensions: Optional[Dict[str, Any]] = None
    status_code: int = 200
    duration_ms: float = 0.0

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    @property
    def is_valid(self) -> bool:
        return self.data is not None and not self.has_errors


@dataclass
class GraphQLRequest:
    """GraphQL request configuration."""
    query: str
    variables: Optional[Dict[str, Any]] = None
    operation_name: Optional[str] = None
    operation_type: OperationType = OperationType.QUERY
    headers: Optional[Dict[str, str]] = None
    timeout: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0


@dataclass
class SchemaField:
    """GraphQL schema field."""
    name: str
    type_name: str
    is_required: bool
    is_list: bool
    args: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None


@dataclass
class SchemaType:
    """GraphQL schema type."""
    name: str
    kind: str
    fields: Dict[str, SchemaField] = field(default_factory=dict)
    input_fields: Dict[str, Any] = field(default_factory=dict)
    enum_values: List[str] = field(default_factory=list)
    possible_types: List[str] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class IntrospectionResult:
    """GraphQL introspection result."""
    types: Dict[str, SchemaType] = field(default_factory=dict)
    query_type: Optional[str] = None
    mutation_type: Optional[str] = None
    subscription_type: Optional[str] = None
    directives: Dict[str, Any] = field(default_factory=dict)


class CacheEntry:
    """Cache entry with TTL support."""

    def __init__(self, value: Any, ttl: float):
        self.value = value
        self.expires_at = time.time() + ttl

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at


class GraphQLAction:
    """GraphQL client action handler.

    This class provides a full-featured GraphQL client with:
    - Query, mutation, and subscription support
    - Automatic retry with exponential backoff
    - Response caching
    - Schema introspection
    - Batching and deduplication

    Example:
        action = GraphQLAction(endpoint="https://api.example.com/graphql")
        result = await action.execute(
            query="{ user(id: 1) { name email } }"
        )
    """

    def __init__(
        self,
        endpoint: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 30.0,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
        cache_ttl: float = 300.0,
        cache_enabled: bool = True,
        default_operation_type: OperationType = OperationType.QUERY,
    ):
        """Initialize GraphQL action.

        Args:
            endpoint: GraphQL endpoint URL
            headers: Default headers for all requests
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts
            retry_delay: Delay between retries
            cache_ttl: Cache TTL in seconds
            cache_enabled: Enable response caching
            default_operation_type: Default operation type
        """
        self.endpoint = endpoint
        self.headers = headers or {}
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.cache_ttl = cache_ttl
        self.cache_enabled = cache_enabled
        self.default_operation_type = default_operation_type

        self._cache: Dict[str, CacheEntry] = {}
        self._introspection_cache: Optional[IntrospectionResult] = None
        self._client: Optional[httpx.AsyncClient] = None

        if not HTTX_AVAILABLE:
            logger.warning("httpx not available, HTTP features disabled")

    def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers=self.headers
            )
        return self._client

    def _generate_cache_key(self, request: GraphQLRequest) -> str:
        """Generate cache key for request."""
        key_data = json.dumps({
            "query": request.query,
            "variables": request.variables,
            "operation_name": request.operation_name
        }, sort_keys=True)
        return hashlib.sha256(key_data.encode()).hexdigest()

    def _classify_error(self, error: Dict[str, Any]) -> ErrorClassification:
        """Classify GraphQL error."""
        extensions = error.get("extensions", {})
        code = extensions.get("code", "")

        if code == "AUTHENTICATION_REQUIRED":
            return ErrorClassification.AUTHORIZATION
        elif code == "FORBIDDEN":
            return ErrorClassification.AUTHORIZATION
        elif code == "VALIDATION_ERROR":
            return ErrorClassification.VALIDATION
        elif code == "GRAPHQL_ERROR":
            return ErrorClassification.GRAPHQL
        else:
            return ErrorClassification.UNKNOWN

    def _parse_response(self, response_data: Dict[str, Any], status_code: int, duration_ms: float) -> GraphQLResponse:
        """Parse GraphQL response."""
        errors = []

        if "errors" in response_data:
            for error in response_data["errors"]:
                errors.append(GraphQLError(
                    message=error.get("message", ""),
                    path=error.get("path"),
                    locations=error.get("locations"),
                    extensions=error.get("extensions"),
                    classification=self._classify_error(error)
                ))

        return GraphQLResponse(
            data=response_data.get("data"),
            errors=errors,
            extensions=response_data.get("extensions"),
            status_code=status_code,
            duration_ms=duration_ms
        )

    async def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        bypass_cache: bool = False,
        retry_attempts: Optional[int] = None,
    ) -> GraphQLResponse:
        """Execute GraphQL query or mutation.

        Args:
            query: GraphQL query string
            variables: Query variables
            operation_name: Operation name
            headers: Additional headers
            bypass_cache: Skip cache lookup
            retry_attempts: Override default retry attempts

        Returns:
            GraphQL response
        """
        request = GraphQLRequest(
            query=query,
            variables=variables,
            operation_name=operation_name,
            headers=headers,
            retry_attempts=retry_attempts if retry_attempts is not None else self.retry_attempts
        )

        cache_key = self._generate_cache_key(request)
        if self.cache_enabled and not bypass_cache:
            cached = self._cache.get(cache_key)
            if cached and not cached.is_expired:
                logger.debug(f"Cache hit for {cache_key[:8]}")
                return cached.value

        last_error = None
        attempts = request.retry_attempts

        for attempt in range(attempts):
            try:
                result = await self._execute_request(request)

                if self.cache_enabled and result.is_valid:
                    self._cache[cache_key] = CacheEntry(result, self.cache_ttl)

                return result

            except httpx.TimeoutException as e:
                last_error = e
                logger.warning(f"Request timeout on attempt {attempt + 1}")
            except httpx.HTTPStatusError as e:
                last_error = e
                if e.response.status_code >= 500:
                    logger.warning(f"Server error {e.response.status_code} on attempt {attempt + 1}")
                else:
                    raise
            except Exception as e:
                last_error = e
                logger.warning(f"Request failed on attempt {attempt + 1}: {e}")

            if attempt < attempts - 1:
                delay = self.retry_delay * (2 ** attempt)
                await asyncio.sleep(delay)

        return GraphQLResponse(
            errors=[GraphQLError(
                message=f"Request failed after {attempts} attempts: {last_error}",
                classification=ErrorClassification.TRANSPORT
            )]
        )

    async def _execute_request(self, request: GraphQLRequest) -> GraphQLResponse:
        """Execute single GraphQL request."""
        start_time = time.time()

        body = {
            "query": request.query
        }
        if request.variables:
            body["variables"] = request.variables
        if request.operation_name:
            body["operationName"] = request.operation_name

        headers = dict(self.headers)
        if request.headers:
            headers.update(request.headers)
        headers["Content-Type"] = "application/json"

        client = self._get_client()
        response = await client.post(
            self.endpoint,
            json=body,
            headers=headers,
            timeout=self.timeout
        )

        duration_ms = (time.time() - start_time) * 1000

        if response.status_code == 200:
            response_data = response.json()
            return self._parse_response(response_data, response.status_code, duration_ms)
        else:
            return GraphQLResponse(
                status_code=response.status_code,
                duration_ms=duration_ms,
                errors=[GraphQLError(
                    message=f"HTTP {response.status_code}: {response.text[:500]}",
                    classification=ErrorClassification.TRANSPORT
                )]
            )

    async def query(
        self,
        selection_set: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None,
        **kwargs
    ) -> GraphQLResponse:
        """Execute a query operation.

        Args:
            selection_set: GraphQL selection set
            variables: Query variables
            operation_name: Operation name
            **kwargs: Additional arguments passed to execute

        Returns:
            GraphQL response
        """
        query_str = f"query {operation_name or ''} {{
{selection_set}
}}"
        return await self.execute(query_str, variables, operation_name, **kwargs)

    async def mutate(
        self,
        mutation_name: str,
        input_vars: Dict[str, Any],
        selection_set: str = "{ success }",
        operation_name: Optional[str] = None,
        **kwargs
    ) -> GraphQLResponse:
        """Execute a mutation operation.

        Args:
            mutation_name: Mutation name
            input_vars: Input variables for mutation
            selection_set: Selection set for response
            operation_name: Operation name
            **kwargs: Additional arguments

        Returns:
            GraphQL response
        """
        var_defs = ", ".join(f"${k}: String!" for k in input_vars.keys())
        var_names = ", ".join(f"{k}: ${k}" for k in input_vars.keys())

        mutation_str = f"mutation {operation_name or ''}({var_defs}) {{
{mutation_name}(input: {{{var_names}}}) {selection_set}
}}"
        return await self.execute(mutation_str, input_vars, operation_name, **kwargs)

    async def batch(
        self,
        requests: List[GraphQLRequest]
    ) -> List[GraphQLResponse]:
        """Execute batch of GraphQL operations.

        Args:
            requests: List of GraphQL requests

        Returns:
            List of GraphQL responses
        """
        tasks = [self.execute(
            r.query,
            r.variables,
            r.operation_name,
            r.headers
        ) for r in requests]

        return await asyncio.gather(*tasks, return_exceptions=True)

    async def introspect(self, force: bool = False) -> IntrospectionResult:
        """Introspect GraphQL schema.

        Args:
            force: Force fresh introspection

        Returns:
            Introspection result
        """
        if self._introspection_cache and not force:
            return self._introspection_cache

        introspection_query = """
        query IntrospectionQuery {
            __schema {
                queryType { name }
                mutationType { name }
                subscriptionType { name }
                types {
                    kind
                    name
                    description
                    fields(includeDeprecated: true) {
                        name
                        description
                        args {
                            name
                            description
                            type { name kind ofType { name kind } }
                            defaultValue
                        }
                        type { name kind ofType { name kind } }
                        isDeprecated
                        deprecationReason
                    }
                    inputFields {
                        name
                        description
                        type { name kind ofType { name kind } }
                        defaultValue
                    }
                    enumValues(includeDeprecated: true) {
                        name
                        description
                        isDeprecated
                        deprecationReason
                    }
                    possibleTypes { name kind }
                }
                directives {
                    name
                    description
                    locations
                    args {
                        name
                        description
                        type { name kind ofType { name kind } }
                        defaultValue
                    }
                }
            }
        }
        """

        result = await self.execute(introspection_query)

        if result.has_errors:
            raise RuntimeError(f"Introspection failed: {result.errors[0].message}")

        schema_data = result.data["__schema"]

        types = {}
        for type_data in schema_data["types"]:
            if type_data["name"] in ("__Schema", "__Type", "__TypeKind", "__Directive", "__DirectiveLocation", "__Field", "__InputValue", "__EnumValue", "__Scalar"):
                continue

            fields = {}
            for field_data in type_data.get("fields", []) or []:
                if not field_data:
                    continue

                type_ref = field_data["type"]
                type_name = type_ref.get("name", "")
                of_type = type_ref.get("ofType")
                is_list = of_type is not None
                if is_list:
                    type_name = of_type.get("name", "")

                fields[field_data["name"]] = SchemaField(
                    name=field_data["name"],
                    type_name=type_name,
                    is_required=type_ref.get("kind") == "NON_NULL",
                    is_list=is_list,
                    description=field_data.get("description"),
                    args={}
                )

            types[type_data["name"]] = SchemaType(
                name=type_data["name"],
                kind=type_data["kind"],
                fields=fields,
                description=type_data.get("description"),
                enum_values=[v["name"] for v in type_data.get("enumValues", []) or []],
                possible_types=[t["name"] for t in type_data.get("possibleTypes", []) or []]
            )

        self._introspection_cache = IntrospectionResult(
            types=types,
            query_type=schema_data.get("queryType", {}).get("name"),
            mutation_type=schema_data.get("mutationType", {}).get("name"),
            subscription_type=schema_data.get("subscriptionType", {}).get("name"),
            directives={d["name"]: d for d in schema_data.get("directives", [])}
        )

        return self._introspection_cache

    async def get_type(self, type_name: str) -> Optional[SchemaType]:
        """Get schema type by name.

        Args:
            type_name: Type name

        Returns:
            Schema type or None
        """
        schema = await self.introspect()
        return schema.types.get(type_name)

    async def subscribe(
        self,
        subscription_query: str,
        variables: Optional[Dict[str, Any]] = None,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ) -> "GraphQLSubscription":
        """Start a GraphQL subscription.

        Args:
            subscription_query: Subscription query
            variables: Query variables
            on_event: Callback for received events
            on_error: Callback for errors

        Returns:
            GraphQL subscription handle
        """
        if not WEBSOCKETS_AVAILABLE:
            raise ImportError("websockets library required for subscriptions")

        parsed_url = urlparse(self.endpoint)
        ws_endpoint = f"ws://{parsed_url.netloc}{parsed_url.path}"

        if parsed_url.scheme == "https":
            ws_endpoint = f"wss://{parsed_url.netloc}{parsed_url.path}"

        subscription = GraphQLSubscription(
            endpoint=ws_endpoint,
            query=subscription_query,
            variables=variables,
            headers=self.headers,
            on_event=on_event,
            on_error=on_error
        )

        await subscription.connect()
        return subscription

    def clear_cache(self) -> None:
        """Clear response cache."""
        self._cache.clear()

    async def close(self) -> None:
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None


class GraphQLSubscription:
    """GraphQL subscription handler.

    Manages WebSocket-based subscriptions for real-time updates.
    """

    def __init__(
        self,
        endpoint: str,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        on_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
    ):
        """Initialize subscription.

        Args:
            endpoint: WebSocket endpoint
            query: Subscription query
            variables: Query variables
            headers: Headers for connection
            on_event: Event callback
            on_error: Error callback
        """
        self.endpoint = endpoint
        self.query = query
        self.variables = variables or {}
        self.headers = headers or {}
        self.on_event = on_event
        self.on_error = on_error

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._connection_id: Optional[str] = None

    async def connect(self) -> None:
        """Connect to subscription endpoint."""
        self._ws = await websockets.connect(
            self.endpoint,
            extra_headers=self.headers
        )
        self._running = True

        init_message = {
            "type": "connection_init",
            "payload": {"headers": self.headers}
        }
        await self._ws.send(json.dumps(init_message))

        response = await self._ws.recv()
        resp_data = json.loads(response)

        if resp_data.get("type") == "connection_ack":
            self._connection_id = resp_data.get("payload", {}).get("connectionParams", {}).get("sessionId")

            subscribe_message = {
                "id": str(id(self)),
                "type": "subscribe",
                "payload": {
                    "query": self.query,
                    "variables": self.variables
                }
            }
            await self._ws.send(json.dumps(subscribe_message))

    async def run(self) -> None:
        """Run subscription loop."""
        while self._running:
            try:
                message = await self._ws.recv()
                data = json.loads(message)

                if data["type"] == "next":
                    payload = data["payload"]
                    if self.on_event:
                        self.on_event(payload.get("data"))
                elif data["type"] == "error":
                    error = Exception(f"Subscription error: {data.get('payload')}")
                    if self.on_error:
                        self.on_error(error)
                elif data["type"] == "complete":
                    break

            except websockets.ConnectionClosed:
                break
            except Exception as e:
                if self.on_error:
                    self.on_error(e)
                break

    async def close(self) -> None:
        """Close subscription."""
        self._running = False
        if self._ws:
            await self._ws.close()


_graphql_action_instance: Optional[GraphQLAction] = None


def get_graphql_action(
    endpoint: str,
    **kwargs
) -> GraphQLAction:
    """Get singleton GraphQL action instance.

    Args:
        endpoint: GraphQL endpoint URL
        **kwargs: Additional arguments

    Returns:
        GraphQLAction instance
    """
    global _graphql_action_instance

    if _graphql_action_instance is None:
        _graphql_action_instance = GraphQLAction(endpoint=endpoint, **kwargs)

    return _graphql_action_instance
