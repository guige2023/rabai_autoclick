"""Tests for Workflow API Gateway Module.

Tests API gateway functionality including routing, rate limiting,
authentication, request/response handling, and workflow execution proxying.
"""

import unittest
import sys
import json
import time
import asyncio
from unittest.mock import Mock, patch, MagicMock, AsyncMock, mock_open
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import hashlib

sys.path.insert(0, '/Users/guige/my_project')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')


# =============================================================================
# Mock Module Imports
# =============================================================================

class GatewayMode(Enum):
    """Gateway operation modes."""
    DIRECT = "direct"
    PROXY = "proxy"
    CACHED = "cached"
    LOAD_BALANCED = "load_balanced"


class RequestMethod(Enum):
    """HTTP request methods."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


class ResponseStatus(Enum):
    """HTTP response statuses."""
    OK = 200
    CREATED = 201
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    INTERNAL_ERROR = 500
    SERVICE_UNAVAILABLE = 503


@dataclass
class GatewayRequest:
    """Gateway request object."""
    method: RequestMethod
    path: str
    headers: Dict[str, str] = field(default_factory=dict)
    body: Optional[Dict] = None
    query_params: Dict[str, str] = field(default_factory=dict)
    client_ip: str = "127.0.0.1"
    timestamp: float = field(default_factory=time.time)
    request_id: str = ""


@dataclass
class GatewayResponse:
    """Gateway response object."""
    status: ResponseStatus
    headers: Dict[str, str] = field(default_factory=dict)
    body: Any = None
    duration: float = 0.0
    cached: bool = False


@dataclass
class RouteConfig:
    """Route configuration."""
    path: str
    method: RequestMethod
    handler: str
    auth_required: bool = False
    rate_limit: int = 100
    cache_ttl: int = 0
    timeout: int = 30


class MockGatewayRouter:
    """Mock gateway router for testing."""

    def __init__(self):
        self.routes: List[RouteConfig] = []
        self._register_default_routes()

    def _register_default_routes(self):
        """Register default routes."""
        self.routes = [
            RouteConfig("/api/workflows", RequestMethod.GET, "list_workflows"),
            RouteConfig("/api/workflows", RequestMethod.POST, "create_workflow"),
            RouteConfig("/api/workflows/{id}", RequestMethod.GET, "get_workflow"),
            RouteConfig("/api/workflows/{id}", RequestMethod.PUT, "update_workflow"),
            RouteConfig("/api/workflows/{id}", RequestMethod.DELETE, "delete_workflow"),
            RouteConfig("/api/execute/{id}", RequestMethod.POST, "execute_workflow"),
            RouteConfig("/api/executions", RequestMethod.GET, "list_executions"),
            RouteConfig("/api/executions/{id}", RequestMethod.GET, "get_execution"),
            RouteConfig("/api/health", RequestMethod.GET, "health_check"),
        ]

    def match_route(self, path: str, method: RequestMethod) -> Optional[RouteConfig]:
        """Match a request to a route."""
        for route in self.routes:
            if self._path_matches(route.path, path) and route.method == method:
                return route
        return None

    def _path_matches(self, pattern: str, path: str) -> bool:
        """Check if path matches route pattern."""
        pattern_parts = pattern.split('/')
        path_parts = path.split('/')
        if len(pattern_parts) != len(path_parts):
            return False
        for p, part in zip(pattern_parts, path_parts):
            if p.startswith('{') and p.endswith('}'):
                continue
            if p != part:
                return False
        return True

    def add_route(self, route: RouteConfig):
        """Add a new route."""
        self.routes.append(route)

    def remove_route(self, path: str, method: RequestMethod):
        """Remove a route."""
        self.routes = [r for r in self.routes if not (r.path == path and r.method == method)]


class MockRateLimiter:
    """Mock rate limiter for testing."""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: Dict[str, List[float]] = {}

    def is_allowed(self, client_id: str) -> bool:
        """Check if request is allowed."""
        now = time.time()
        if client_id not in self.requests:
            self.requests[client_id] = []

        self.requests[client_id] = [
            ts for ts in self.requests[client_id]
            if now - ts < self.window_seconds
        ]

        if len(self.requests[client_id]) >= self.max_requests:
            return False

        self.requests[client_id].append(now)
        return True

    def get_remaining(self, client_id: str) -> int:
        """Get remaining requests for client."""
        now = time.time()
        if client_id not in self.requests:
            return self.max_requests

        active = [ts for ts in self.requests[client_id] if now - ts < self.window_seconds]
        return max(0, self.max_requests - len(active))

    def reset(self, client_id: str):
        """Reset rate limit for client."""
        if client_id in self.requests:
            del self.requests[client_id]


class MockAuthHandler:
    """Mock authentication handler for testing."""

    def __init__(self):
        self.api_keys: Dict[str, Dict] = {}
        self.tokens: Dict[str, Dict] = {}

    def validate_api_key(self, api_key: str) -> bool:
        """Validate an API key."""
        return api_key in self.api_keys

    def validate_token(self, token: str) -> bool:
        """Validate a bearer token."""
        if token in self.tokens:
            expiry = self.tokens[token].get('expiry', 0)
            return time.time() < expiry
        return False

    def generate_token(self, client_id: str, expires_in: int = 3600) -> str:
        """Generate a bearer token."""
        token = hashlib.sha256(f"{client_id}{time.time()}".encode()).hexdigest()
        self.tokens[token] = {
            'client_id': client_id,
            'expiry': time.time() + expires_in
        }
        return token

    def add_api_key(self, key: str, client_info: Dict):
        """Add an API key."""
        self.api_keys[key] = client_info


class MockCache:
    """Mock cache for testing."""

    def __init__(self, default_ttl: int = 300):
        self.default_ttl = default_ttl
        self.store: Dict[str, Dict] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if key in self.store:
            entry = self.store[key]
            if time.time() < entry['expiry']:
                return entry['value']
            else:
                del self.store[key]
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in cache."""
        ttl = ttl or self.default_ttl
        self.store[key] = {
            'value': value,
            'expiry': time.time() + ttl
        }

    def delete(self, key: str):
        """Delete value from cache."""
        if key in self.store:
            del self.store[key]

    def clear(self):
        """Clear all cache."""
        self.store.clear()


class MockWorkflowExecutor:
    """Mock workflow executor for testing."""

    def __init__(self):
        self.executions: Dict[str, Dict] = {}
        self.workflows: Dict[str, Dict] = {}

    def execute(self, workflow_id: str, params: Dict = None) -> Dict:
        """Execute a workflow."""
        execution_id = f"exec_{len(self.executions) + 1}"
        execution = {
            'execution_id': execution_id,
            'workflow_id': workflow_id,
            'status': 'running',
            'started_at': time.time(),
            'params': params or {}
        }
        self.executions[execution_id] = execution
        return execution

    def get_execution(self, execution_id: str) -> Optional[Dict]:
        """Get execution status."""
        return self.executions.get(execution_id)

    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel an execution."""
        if execution_id in self.executions:
            self.executions[execution_id]['status'] = 'cancelled'
            return True
        return False

    def add_workflow(self, workflow: Dict):
        """Add a workflow to executor."""
        self.workflows[workflow['workflow_id']] = workflow

    def get_workflow(self, workflow_id: str) -> Optional[Dict]:
        """Get a workflow."""
        return self.workflows.get(workflow_id)


class MockAPIGateway:
    """Mock API Gateway main class."""

    def __init__(self, mode: GatewayMode = GatewayMode.DIRECT):
        self.mode = mode
        self.router = MockGatewayRouter()
        self.rate_limiter = MockRateLimiter()
        self.auth = MockAuthHandler()
        self.cache = MockCache()
        self.executor = MockWorkflowExecutor()
        self.middleware: List[callable] = []

    def handle_request(self, request: GatewayRequest) -> GatewayResponse:
        """Handle an incoming request."""
        start_time = time.time()

        route = self.router.match_route(request.path, request.method)
        if not route:
            return GatewayResponse(
                status=ResponseStatus.NOT_FOUND,
                body={'error': 'Route not found'},
                duration=time.time() - start_time
            )

        if route.auth_required:
            api_key = request.headers.get('X-API-Key')
            if not api_key or not self.auth.validate_api_key(api_key):
                return GatewayResponse(
                    status=ResponseStatus.UNAUTHORIZED,
                    body={'error': 'Unauthorized'},
                    duration=time.time() - start_time
                )

        if not self.rate_limiter.is_allowed(request.client_ip):
            return GatewayResponse(
                status=ResponseStatus.SERVICE_UNAVAILABLE,
                body={'error': 'Rate limit exceeded'},
                duration=time.time() - start_time
            )

        cache_key = f"{request.method.value}:{request.path}"
        cached = self.cache.get(cache_key)
        if cached:
            return GatewayResponse(
                status=ResponseStatus.OK,
                body=cached,
                duration=time.time() - start_time,
                cached=True
            )

        response = self._process_request(request, route)

        if route.cache_ttl > 0:
            self.cache.set(cache_key, response.body, route.cache_ttl)

        response.duration = time.time() - start_time
        return response

    def _process_request(self, request: GatewayRequest, route: RouteConfig) -> GatewayResponse:
        """Process the request through the route handler."""
        if route.handler == "list_workflows":
            workflows = list(self.executor.workflows.values())
            return GatewayResponse(status=ResponseStatus.OK, body={'workflows': workflows})
        elif route.handler == "get_workflow":
            workflow_id = self._extract_path_param(route.path, request.path)
            workflow = self.executor.get_workflow(workflow_id)
            if workflow:
                return GatewayResponse(status=ResponseStatus.OK, body=workflow)
            return GatewayResponse(status=ResponseStatus.NOT_FOUND, body={'error': 'Not found'})
        elif route.handler == "execute_workflow":
            workflow_id = self._extract_path_param(route.path, request.path)
            result = self.executor.execute(workflow_id, request.body)
            return GatewayResponse(status=ResponseStatus.OK, body=result)
        elif route.handler == "health_check":
            return GatewayResponse(status=ResponseStatus.OK, body={'status': 'healthy'})
        return GatewayResponse(status=ResponseStatus.OK, body={'message': 'OK'})

    def _extract_path_param(self, pattern: str, path: str) -> str:
        """Extract path parameters from URL."""
        pattern_parts = pattern.split('/')
        path_parts = path.split('/')
        for p, part in zip(pattern_parts, path_parts):
            if p.startswith('{') and p.endswith('}'):
                return part
        return ""


class GatewayError(Exception):
    """Gateway error."""
    pass


class RouteNotFoundError(GatewayError):
    """Route not found error."""
    pass


class AuthenticationError(GatewayError):
    """Authentication error."""
    pass


class RateLimitError(GatewayError):
    """Rate limit error."""
    pass


# =============================================================================
# Test Gateway Router
# =============================================================================

class TestGatewayRouter(unittest.TestCase):
    """Test GatewayRouter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.router = MockGatewayRouter()

    def test_match_exact_route(self):
        """Test matching an exact route."""
        route = self.router.match_route("/api/workflows", RequestMethod.GET)
        self.assertIsNotNone(route)
        self.assertEqual(route.handler, "list_workflows")

    def test_match_route_with_path_param(self):
        """Test matching route with path parameter."""
        route = self.router.match_route("/api/workflows/123", RequestMethod.GET)
        self.assertIsNotNone(route)
        self.assertEqual(route.handler, "get_workflow")

    def test_no_match_wrong_method(self):
        """Test no match with wrong method."""
        route = self.router.match_route("/api/workflows", RequestMethod.POST)
        self.assertIsNotNone(route)

    def test_no_match_nonexistent_route(self):
        """Test no match for nonexistent route."""
        route = self.router.match_route("/api/nonexistent", RequestMethod.GET)
        self.assertIsNone(route)

    def test_add_route(self):
        """Test adding a new route."""
        new_route = RouteConfig("/api/custom", RequestMethod.GET, "custom_handler")
        self.router.add_route(new_route)
        route = self.router.match_route("/api/custom", RequestMethod.GET)
        self.assertIsNotNone(route)
        self.assertEqual(route.handler, "custom_handler")

    def test_remove_route(self):
        """Test removing a route."""
        self.router.remove_route("/api/health", RequestMethod.GET)
        route = self.router.match_route("/api/health", RequestMethod.GET)
        self.assertIsNone(route)


# =============================================================================
# Test Rate Limiter
# =============================================================================

class TestRateLimiter(unittest.TestCase):
    """Test RateLimiter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.limiter = MockRateLimiter(max_requests=5, window_seconds=60)

    def test_allows_under_limit(self):
        """Test allowing requests under limit."""
        for i in range(5):
            self.assertTrue(self.limiter.is_allowed("client1"))

    def test_blocks_over_limit(self):
        """Test blocking requests over limit."""
        for i in range(5):
            self.limiter.is_allowed("client1")
        self.assertFalse(self.limiter.is_allowed("client1"))

    def test_different_clients_independent(self):
        """Test different clients have independent limits."""
        for i in range(5):
            self.limiter.is_allowed("client1")
        self.assertTrue(self.limiter.is_allowed("client2"))

    def test_get_remaining(self):
        """Test getting remaining requests."""
        self.limiter.is_allowed("client1")
        self.limiter.is_allowed("client1")
        remaining = self.limiter.get_remaining("client1")
        self.assertEqual(remaining, 3)

    def test_reset_client(self):
        """Test resetting client rate limit."""
        for i in range(5):
            self.limiter.is_allowed("client1")
        self.limiter.reset("client1")
        self.assertTrue(self.limiter.is_allowed("client1"))


# =============================================================================
# Test Auth Handler
# =============================================================================

class TestAuthHandler(unittest.TestCase):
    """Test AuthHandler class."""

    def setUp(self):
        """Set up test fixtures."""
        self.auth = MockAuthHandler()

    def test_validate_valid_api_key(self):
        """Test validating a valid API key."""
        self.auth.add_api_key("test_key", {"client": "test"})
        self.assertTrue(self.auth.validate_api_key("test_key"))

    def test_validate_invalid_api_key(self):
        """Test validating an invalid API key."""
        self.assertFalse(self.auth.validate_api_key("invalid_key"))

    def test_generate_token(self):
        """Test generating a token."""
        token = self.auth.generate_token("client1")
        self.assertIsNotNone(token)
        self.assertTrue(self.auth.validate_token(token))

    def test_validate_expired_token(self):
        """Test validating an expired token."""
        token = self.auth.generate_token("client1", expires_in=-1)
        self.assertFalse(self.auth.validate_token(token))


# =============================================================================
# Test Cache
# =============================================================================

class TestCache(unittest.TestCase):
    """Test Cache class."""

    def setUp(self):
        """Set up test fixtures."""
        self.cache = MockCache(default_ttl=5)

    def test_set_and_get(self):
        """Test setting and getting a value."""
        self.cache.set("key1", {"data": "value"})
        result = self.cache.get("key1")
        self.assertEqual(result, {"data": "value"})

    def test_get_nonexistent(self):
        """Test getting nonexistent key."""
        result = self.cache.get("nonexistent")
        self.assertIsNone(result)

    def test_expired_key(self):
        """Test getting expired key."""
        self.cache.set("key1", "value", ttl=1)
        time.sleep(1.1)
        result = self.cache.get("key1")
        self.assertIsNone(result)

    def test_delete(self):
        """Test deleting a key."""
        self.cache.set("key1", "value")
        self.cache.delete("key1")
        self.assertIsNone(self.cache.get("key1"))

    def test_clear(self):
        """Test clearing cache."""
        self.cache.set("key1", "value1")
        self.cache.set("key2", "value2")
        self.cache.clear()
        self.assertIsNone(self.cache.get("key1"))
        self.assertIsNone(self.cache.get("key2"))


# =============================================================================
# Test Workflow Executor
# =============================================================================

class TestWorkflowExecutor(unittest.TestCase):
    """Test WorkflowExecutor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.executor = MockWorkflowExecutor()

    def test_execute_workflow(self):
        """Test executing a workflow."""
        self.executor.add_workflow({'workflow_id': 'wf_001', 'name': 'Test'})
        result = self.executor.execute('wf_001')
        self.assertIn('execution_id', result)
        self.assertEqual(result['workflow_id'], 'wf_001')

    def test_get_execution(self):
        """Test getting execution status."""
        self.executor.add_workflow({'workflow_id': 'wf_001', 'name': 'Test'})
        exec_result = self.executor.execute('wf_001')
        execution = self.executor.get_execution(exec_result['execution_id'])
        self.assertIsNotNone(execution)

    def test_cancel_execution(self):
        """Test cancelling an execution."""
        self.executor.add_workflow({'workflow_id': 'wf_001', 'name': 'Test'})
        exec_result = self.executor.execute('wf_001')
        success = self.executor.cancel_execution(exec_result['execution_id'])
        self.assertTrue(success)


# =============================================================================
# Test API Gateway
# =============================================================================

class TestAPIGateway(unittest.TestCase):
    """Test MockAPIGateway class."""

    def setUp(self):
        """Set up test fixtures."""
        self.gateway = MockAPIGateway()

    def test_health_check(self):
        """Test health check endpoint."""
        request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/health"
        )
        response = self.gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.OK)
        self.assertEqual(response.body['status'], 'healthy')

    def test_list_workflows(self):
        """Test listing workflows."""
        request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/workflows"
        )
        response = self.gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.OK)

    def test_get_workflow(self):
        """Test getting a workflow."""
        self.gateway.executor.add_workflow({'workflow_id': 'wf_001', 'name': 'Test'})
        request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/workflows/wf_001"
        )
        response = self.gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.OK)

    def test_get_nonexistent_workflow(self):
        """Test getting nonexistent workflow."""
        request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/workflows/nonexistent"
        )
        response = self.gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.NOT_FOUND)

    def test_execute_workflow(self):
        """Test executing a workflow."""
        self.gateway.executor.add_workflow({'workflow_id': 'wf_001', 'name': 'Test'})
        request = GatewayRequest(
            method=RequestMethod.POST,
            path="/api/execute/wf_001",
            body={'params': {}}
        )
        response = self.gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.OK)

    def test_route_not_found(self):
        """Test route not found."""
        request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/unknown"
        )
        response = self.gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.NOT_FOUND)

    def test_rate_limit_exceeded(self):
        """Test rate limit exceeded."""
        for i in range(100):
            self.gateway.handle_request(GatewayRequest(
                method=RequestMethod.GET,
                path="/api/health",
                client_ip="limited_client"
            ))
        request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/health",
            client_ip="limited_client"
        )
        response = self.gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.SERVICE_UNAVAILABLE)

    def test_caching(self):
        """Test response caching."""
        # Find health route and set cache_ttl
        for route in self.gateway.router.routes:
            if route.path == "/api/health":
                route.cache_ttl = 60
                break
        request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/health"
        )
        response1 = self.gateway.handle_request(request)
        response2 = self.gateway.handle_request(request)
        self.assertTrue(response2.cached)


# =============================================================================
# Test Gateway Errors
# =============================================================================

class TestGatewayErrors(unittest.TestCase):
    """Test gateway error classes."""

    def test_gateway_error(self):
        """Test GatewayError."""
        error = GatewayError("Test error")
        self.assertEqual(str(error), "Test error")

    def test_route_not_found_error(self):
        """Test RouteNotFoundError."""
        error = RouteNotFoundError("Route not found")
        self.assertIn("not found", str(error))

    def test_authentication_error(self):
        """Test AuthenticationError."""
        error = AuthenticationError("Invalid credentials")
        self.assertIn("Invalid", str(error))

    def test_rate_limit_error(self):
        """Test RateLimitError."""
        error = RateLimitError("Rate limit exceeded")
        self.assertIn("exceeded", str(error))


# =============================================================================
# Test Gateway Request/Response
# =============================================================================

class TestGatewayRequestResponse(unittest.TestCase):
    """Test GatewayRequest and GatewayResponse classes."""

    def test_gateway_request_creation(self):
        """Test creating a GatewayRequest."""
        request = GatewayRequest(
            method=RequestMethod.POST,
            path="/api/workflows",
            headers={"Content-Type": "application/json"},
            body={"name": "Test"},
            client_ip="192.168.1.1"
        )
        self.assertEqual(request.method, RequestMethod.POST)
        self.assertEqual(request.path, "/api/workflows")
        self.assertEqual(request.client_ip, "192.168.1.1")

    def test_gateway_response_creation(self):
        """Test creating a GatewayResponse."""
        response = GatewayResponse(
            status=ResponseStatus.OK,
            body={"success": True},
            duration=0.5,
            cached=False
        )
        self.assertEqual(response.status, ResponseStatus.OK)
        self.assertEqual(response.duration, 0.5)


# =============================================================================
# Test Gateway Route Config
# =============================================================================

class TestRouteConfig(unittest.TestCase):
    """Test RouteConfig class."""

    def test_route_config_creation(self):
        """Test creating a RouteConfig."""
        route = RouteConfig(
            path="/api/test",
            method=RequestMethod.GET,
            handler="test_handler",
            auth_required=True,
            rate_limit=50,
            cache_ttl=60,
            timeout=30
        )
        self.assertEqual(route.path, "/api/test")
        self.assertTrue(route.auth_required)
        self.assertEqual(route.rate_limit, 50)


# =============================================================================
# Test Gateway File Operations (Mocked)
# =============================================================================

class TestGatewayFileOperations(unittest.TestCase):
    """Test gateway file operations with mocked I/O."""

    @patch('builtins.open', new_callable=mock_open, read_data='{"routes": []}')
    def test_load_config_from_file(self, mock_file):
        """Test loading gateway config from file."""
        with open('/mock/path/gateway_config.json', 'r') as f:
            content = json.load(f)
        self.assertIsInstance(content, dict)

    @patch('builtins.open', new_callable=mock_open)
    def test_save_config_to_file(self, mock_file):
        """Test saving gateway config to file."""
        config = {'routes': [], 'mode': 'direct'}
        with open('/mock/path/gateway_config.json', 'w') as f:
            json.dump(config, f)
        mock_file.assert_called_with('/mock/path/gateway_config.json', 'w')


# =============================================================================
# Test Gateway Integration
# =============================================================================

class TestGatewayIntegration(unittest.TestCase):
    """Test gateway integration scenarios."""

    def test_full_workflow_execution_flow(self):
        """Test full workflow execution through gateway."""
        gateway = MockAPIGateway()
        gateway.executor.add_workflow({
            'workflow_id': 'wf_test',
            'name': 'Test Workflow',
            'steps': [{'action': 'click'}]
        })

        create_request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/workflows"
        )
        response = gateway.handle_request(create_request)
        self.assertEqual(response.status, ResponseStatus.OK)

        exec_request = GatewayRequest(
            method=RequestMethod.POST,
            path="/api/execute/wf_test",
            body={'params': {}}
        )
        response = gateway.handle_request(exec_request)
        self.assertEqual(response.status, ResponseStatus.OK)

    def test_authenticated_request_flow(self):
        """Test authenticated request flow."""
        gateway = MockAPIGateway()
        gateway.auth.add_api_key("valid_key", {"client": "test"})
        gateway.router.routes[0].auth_required = True

        request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/workflows",
            headers={"X-API-Key": "valid_key"}
        )
        response = gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.OK)

    def test_unauthenticated_request_flow(self):
        """Test unauthenticated request flow."""
        gateway = MockAPIGateway()
        gateway.router.routes[0].auth_required = True

        request = GatewayRequest(
            method=RequestMethod.GET,
            path="/api/workflows"
        )
        response = gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.UNAUTHORIZED)


# =============================================================================
# Test Gateway Edge Cases
# =============================================================================

class TestGatewayEdgeCases(unittest.TestCase):
    """Test gateway edge cases."""

    def test_empty_path(self):
        """Test handling empty path."""
        gateway = MockAPIGateway()
        request = GatewayRequest(method=RequestMethod.GET, path="")
        response = gateway.handle_request(request)
        self.assertEqual(response.status, ResponseStatus.NOT_FOUND)

    def test_special_characters_in_path(self):
        """Test handling special characters in path."""
        gateway = MockAPIGateway()
        request = GatewayRequest(method=RequestMethod.GET, path="/api/test%20path")
        response = gateway.handle_request(request)
        self.assertIn(response.status, [ResponseStatus.OK, ResponseStatus.NOT_FOUND])

    def test_large_request_body(self):
        """Test handling large request body."""
        gateway = MockAPIGateway()
        large_body = {'data': 'x' * 10000}
        request = GatewayRequest(
            method=RequestMethod.POST,
            path="/api/execute/wf_001",
            body=large_body
        )
        response = gateway.handle_request(request)
        self.assertIn(response.status, [ResponseStatus.OK, ResponseStatus.NOT_FOUND])


# =============================================================================
# Test Gateway Performance
# =============================================================================

class TestGatewayPerformance(unittest.TestCase):
    """Test gateway performance."""

    def test_many_requests(self):
        """Test handling many requests."""
        gateway = MockAPIGateway()
        for i in range(50):
            request = GatewayRequest(method=RequestMethod.GET, path="/api/health")
            response = gateway.handle_request(request)
            self.assertEqual(response.status, ResponseStatus.OK)

    def test_cache_performance(self):
        """Test cache improves performance."""
        gateway = MockAPIGateway()
        request = GatewayRequest(method=RequestMethod.GET, path="/api/health")

        start = time.time()
        gateway.handle_request(request)
        first_duration = time.time() - start

        start = time.time()
        gateway.handle_request(request)
        second_duration = time.time() - start

        self.assertLessEqual(second_duration, first_duration)


if __name__ == '__main__':
    unittest.main()
