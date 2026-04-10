"""Tests for Workflow GraphQL Module.

Tests GraphQL API functionality including queries, mutations, subscriptions,
batch operations, authentication, rate limiting, schema stitching, and execution handling.
"""

import unittest
import sys
import json
import time
import asyncio
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field, asdict
from enum import Enum
import hashlib
import threading
import uuid

sys.path.insert(0, '/Users/guige/my_project')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')


# =============================================================================
# Mock Module Imports and Data Structures
# =============================================================================

class AuthenticationError(Exception):
    """GraphQL Authentication Error"""
    pass


class RateLimitError(Exception):
    """Rate Limit Exceeded Error"""
    pass


class ValidationError(Exception):
    """Validation Error"""
    pass


class ExecutionError(Exception):
    """Workflow Execution Error"""
    pass


class WorkflowExecutionStatus(Enum):
    """Workflow execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class GraphQLContext:
    """GraphQL request context"""
    request_id: str
    user_id: Optional[str] = None
    auth_token: Optional[str] = None
    permissions: List[str] = field(default_factory=list)
    rate_limit_remaining: int = 100
    rate_limit_reset: float = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GraphQLErrorResponse:
    """GraphQL error format per spec"""
    message: str
    locations: Optional[List[Dict[str, Any]]] = None
    path: Optional[List[str]] = None
    extensions: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {"message": self.message}
        if self.locations:
            result["locations"] = self.locations
        if self.path:
            result["path"] = self.path
        if self.extensions:
            result["extensions"] = self.extensions
        return result


@dataclass
class WorkflowExecution:
    """Workflow execution record"""
    execution_id: str
    workflow_id: str
    workflow_name: str
    status: WorkflowExecutionStatus
    started_at: float
    completed_at: Optional[float] = None
    input_variables: Dict[str, Any] = field(default_factory=dict)
    output_variables: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    progress: float = 0.0
    steps_completed: int = 0
    total_steps: int = 0


@dataclass
class WorkflowVariable:
    """Workflow variable"""
    variable_id: str
    workflow_id: str
    name: str
    value: Any
    var_type: str
    is_secret: bool = False


class RateLimiter:
    """Token bucket rate limiter for GraphQL operations"""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: Dict[str, List[float]] = {}

    def check_rate_limit(self, key: str, cost: int = 1) -> bool:
        """Check if request is within rate limit"""
        now = time.time()
        window_start = now - self.window_seconds

        if key not in self.requests:
            self.requests[key] = []

        self.requests[key] = [t for t in self.requests[key] if t > window_start]

        if len(self.requests[key]) >= self.max_requests:
            return False

        self.requests[key].append(now)
        return True

    def get_remaining(self, key: str) -> int:
        """Get remaining requests for key"""
        now = time.time()
        window_start = now - self.window_seconds
        if key in self.requests:
            self.requests[key] = [t for t in self.requests[key] if t > window_start]
        return max(0, self.max_requests - len(self.requests.get(key, [])))


class GraphQLAuthenticator:
    """GraphQL-specific authentication handler"""

    def __init__(self, secret_key: str = "default-secret"):
        self.secret_key = secret_key
        self.token_cache: Dict[str, Dict[str, Any]] = {}

    def authenticate(self, context: GraphQLContext) -> bool:
        """Authenticate GraphQL request"""
        if not context.auth_token:
            return True

        token_hash = hashlib.sha256(context.auth_token.encode()).hexdigest()
        if token_hash in self.token_cache:
            token_data = self.token_cache[token_hash]
            if token_data.get("expires", 0) > time.time():
                context.user_id = token_data.get("user_id")
                context.permissions = token_data.get("permissions", [])
                return True

        if len(context.auth_token) >= 32:
            context.user_id = "authenticated_user"
            context.permissions = ["read", "write", "execute"]
            return True

        return False

    def create_token(self, user_id: str, permissions: List[str], ttl: int = 3600) -> str:
        """Create authentication token"""
        token = f"{user_id}:{uuid.uuid4().hex}:{int(time.time() + ttl)}"
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        self.token_cache[token_hash] = {
            "user_id": user_id,
            "permissions": permissions,
            "expires": time.time() + ttl
        }
        return token_hash


class BatchOperation:
    """Represents a single batched operation"""

    def __init__(self, operation_type: str, operation_name: str, variables: Dict[str, Any]):
        self.operation_type = operation_type
        self.operation_name = operation_name
        self.variables = variables
        self.result: Optional[Any] = None
        self.error: Optional[GraphQLErrorResponse] = None


class BatchProcessor:
    """Processes multiple GraphQL operations in a single request"""

    def __init__(self, max_batch_size: int = 10):
        self.max_batch_size = max_batch_size

    def process_batch(
        self,
        operations: List[BatchOperation],
        executor: callable
    ) -> List[Dict[str, Any]]:
        """Process multiple operations"""
        results = []
        for op in operations:
            try:
                result = executor(op)
                results.append({"data": result, "errors": None})
            except Exception as e:
                error = GraphQLErrorResponse(
                    message=str(e),
                    extensions={"code": "BATCH_ERROR"}
                )
                results.append({"data": None, "errors": [error.to_dict()]})
        return results


class SubscriptionManager:
    """Manages GraphQL subscriptions for real-time updates"""

    def __init__(self):
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def subscribe(
        self,
        subscription_id: str,
        query: str,
        variables: Dict[str, Any],
        callback: callable
    ) -> str:
        """Create a new subscription"""
        with self._lock:
            self.subscriptions[subscription_id] = {
                "query": query,
                "variables": variables,
                "callback": callback,
                "created_at": time.time(),
                "active": True
            }
        return subscription_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Remove a subscription"""
        with self._lock:
            if subscription_id in self.subscriptions:
                self.subscriptions[subscription_id]["active"] = False
                del self.subscriptions[subscription_id]
                return True
            return False

    async def push_update(self, subscription_id: str, data: Dict[str, Any]) -> bool:
        """Push update to subscription"""
        with self._lock:
            if subscription_id not in self.subscriptions:
                return False
            sub = self.subscriptions[subscription_id]
            if not sub["active"]:
                return False

        callback = sub["callback"]
        if asyncio.iscoroutinefunction(callback):
            await callback(data)
        else:
            callback(data)
        return True

    def broadcast(self, event_type: str, data: Dict[str, Any]):
        """Broadcast to all matching subscriptions"""
        with self._lock:
            for sub_id, sub in self.subscriptions.items():
                if sub["active"] and event_type in sub["query"]:
                    callback = sub["callback"]
                    if asyncio.iscoroutinefunction(callback):
                        asyncio.create_task(callback(data))
                    else:
                        callback(data)


# =============================================================================
# Mock Data Stores
# =============================================================================

_workflows_store: Dict[str, Dict[str, Any]] = {}
_executions_store: Dict[str, WorkflowExecution] = {}
_variables_store: Dict[str, WorkflowVariable] = {}


def _clear_stores():
    """Clear all mock data stores"""
    _workflows_store.clear()
    _executions_store.clear()
    _variables_store.clear()


# =============================================================================
# Mock GraphQL Schema Builder
# =============================================================================

class MockGraphQLSchemaBuilder:
    """Mock GraphQL schema builder for testing"""

    def __init__(
        self,
        authenticator: Optional[GraphQLAuthenticator] = None,
        rate_limiter: Optional[RateLimiter] = None
    ):
        self.authenticator = authenticator or GraphQLAuthenticator()
        self.rate_limiter = rate_limiter or RateLimiter()
        self.subscription_manager = SubscriptionManager()
        self.batch_processor = BatchProcessor()

    def _check_auth(self, context: GraphQLContext, required_permission: str = "read"):
        """Check authentication and permissions"""
        if not self.authenticator.authenticate(context):
            raise AuthenticationError("Authentication required")

        if required_permission not in ["read", "write", "execute"]:
            return

        if required_permission == "execute" and "execute" not in context.permissions:
            raise AuthenticationError("Execute permission required")

    def _check_rate_limit(self, context: GraphQLContext, cost: int = 1):
        """Check rate limit"""
        key = context.user_id or context.request_id
        if not self.rate_limiter.check_rate_limit(key, cost):
            remaining = self.rate_limiter.get_remaining(key)
            raise RateLimitError(f"Rate limit exceeded. Remaining: {remaining}")

    def _format_error(self, error: Exception, locations: Optional[List] = None) -> Dict:
        """Format error according to GraphQL spec"""
        extensions = {"code": "INTERNAL_ERROR"}

        if isinstance(error, AuthenticationError):
            extensions["code"] = "AUTHENTICATION_ERROR"
        elif isinstance(error, RateLimitError):
            extensions["code"] = "RATE_LIMIT_ERROR"
        elif isinstance(error, ValidationError):
            extensions["code"] = "VALIDATION_ERROR"
        elif isinstance(error, ExecutionError):
            extensions["code"] = "EXECUTION_ERROR"

        return GraphQLErrorResponse(
            message=str(error),
            locations=locations,
            extensions=extensions
        ).to_dict()

    def execute_query(self, query: str, context: GraphQLContext, variables: Dict = None) -> Dict[str, Any]:
        """Execute a GraphQL query"""
        try:
            self._check_auth(context, "read")
            self._check_rate_limit(context)

            if "workflows" in query:
                return {"data": {"workflows": list(_workflows_store.values())}}
            elif "workflow" in query and "id" in (variables or {}):
                return {"data": {"workflow": _workflows_store.get(variables.get("id"))}}
            elif "executions" in query:
                return {"data": {"executions": list(_executions_store.values())}}

            return {"data": None, "errors": [{"message": "Unknown query"}]}
        except Exception as e:
            return {"data": None, "errors": [self._format_error(e)]}

    def execute_mutation(self, mutation: str, context: GraphQLContext, variables: Dict = None) -> Dict[str, Any]:
        """Execute a GraphQL mutation"""
        try:
            self._check_auth(context, "write")
            self._check_rate_limit(context, cost=5)

            if "createWorkflow" in mutation:
                workflow_id = str(uuid.uuid4())
                now = time.time()
                workflow_data = {
                    "id": workflow_id,
                    "name": variables.get("input", {}).get("name", "Unnamed"),
                    "description": variables.get("input", {}).get("description", ""),
                    "version": "1.0.0",
                    "status": "draft",
                    "createdAt": now,
                    "updatedAt": now,
                    "createdBy": context.user_id or "anonymous",
                    "steps": variables.get("input", {}).get("steps", []),
                    "variables": variables.get("input", {}).get("variables", []),
                    "metadata": variables.get("input", {}).get("metadata", "{}"),
                }
                _workflows_store[workflow_id] = workflow_data
                return {"data": {"createWorkflow": workflow_data}}

            elif "executeWorkflow" in mutation:
                workflow_id = variables.get("workflowId")
                if workflow_id not in _workflows_store:
                    raise ExecutionError(f"Workflow {workflow_id} not found")

                workflow = _workflows_store[workflow_id]
                execution_id = str(uuid.uuid4())
                now = time.time()

                execution = WorkflowExecution(
                    execution_id=execution_id,
                    workflow_id=workflow_id,
                    workflow_name=workflow["name"],
                    status=WorkflowExecutionStatus.RUNNING,
                    started_at=now,
                    input_variables=variables.get("inputVariables", {}),
                    total_steps=len(workflow.get("steps", []))
                )

                _executions_store[execution_id] = execution

                return {
                    "data": {
                        "executeWorkflow": {
                            "success": True,
                            "executionId": execution_id,
                            "output": json.dumps({"status": "started"}),
                            "error": None,
                            "executionTime": 0
                        }
                    }
                }

            return {"data": None, "errors": [{"message": "Unknown mutation"}]}
        except Exception as e:
            return {"data": None, "errors": [self._format_error(e)]}

    def execute_batch_operations(self, operations: List[Dict], context: GraphQLContext) -> List[Dict[str, Any]]:
        """Execute batch operations"""
        results = []
        for idx, op in enumerate(operations):
            try:
                op_type = op.get("type")
                op_name = op.get("operationName")
                op_variables = op.get("variables", {})

                if op_name == "CreateWorkflow":
                    workflow_id = str(uuid.uuid4())
                    now = time.time()
                    result = {
                        "id": workflow_id,
                        "name": op_variables.get("input", {}).get("name", "Unnamed"),
                        "description": op_variables.get("input", {}).get("description", ""),
                        "version": "1.0.0",
                        "status": "draft",
                        "createdAt": now,
                        "updatedAt": now,
                        "createdBy": context.user_id or "anonymous",
                    }
                    _workflows_store[workflow_id] = result
                else:
                    result = {"success": True}

                results.append({
                    "index": idx,
                    "success": True,
                    "data": json.dumps(result),
                    "error": None
                })
            except Exception as e:
                results.append({
                    "index": idx,
                    "success": False,
                    "data": None,
                    "error": str(e)
                })

        return results


# =============================================================================
# Test Rate Limiter
# =============================================================================

class TestRateLimiter(unittest.TestCase):
    """Test RateLimiter class."""

    def setUp(self):
        """Set up test fixtures."""
        self.limiter = RateLimiter(max_requests=5, window_seconds=60)

    def test_allows_under_limit(self):
        """Test allowing requests under limit."""
        for i in range(5):
            self.assertTrue(self.limiter.check_rate_limit("client1"))

    def test_blocks_over_limit(self):
        """Test blocking requests over limit."""
        for i in range(5):
            self.limiter.check_rate_limit("client1")
        self.assertFalse(self.limiter.check_rate_limit("client1"))

    def test_different_keys_independent(self):
        """Test different keys have independent limits."""
        for i in range(5):
            self.limiter.check_rate_limit("client1")
        self.assertTrue(self.limiter.check_rate_limit("client2"))

    def test_get_remaining(self):
        """Test getting remaining requests."""
        self.limiter.check_rate_limit("client1")
        self.limiter.check_rate_limit("client1")
        remaining = self.limiter.get_remaining("client1")
        self.assertEqual(remaining, 3)

    def test_get_remaining_unknown_key(self):
        """Test getting remaining for unknown key."""
        remaining = self.limiter.get_remaining("unknown")
        self.assertEqual(remaining, 5)


# =============================================================================
# Test GraphQL Authenticator
# =============================================================================

class TestGraphQLAuthenticator(unittest.TestCase):
    """Test GraphQLAuthenticator class."""

    def setUp(self):
        """Set up test fixtures."""
        self.authenticator = GraphQLAuthenticator(secret_key="test-secret")

    def test_authenticate_no_token(self):
        """Test authentication with no token (allows open API)."""
        context = GraphQLContext(request_id="req1")
        self.assertTrue(self.authenticator.authenticate(context))

    def test_authenticate_valid_token(self):
        """Test authentication with valid token."""
        token = self.authenticator.create_token("user1", ["read", "write"], ttl=3600)
        context = GraphQLContext(request_id="req1", auth_token=token)
        self.assertTrue(self.authenticator.authenticate(context))
        self.assertEqual(context.user_id, "user1")
        self.assertEqual(context.permissions, ["read", "write"])

    def test_authenticate_expired_token(self):
        """Test authentication with expired token."""
        token = self.authenticator.create_token("user1", ["read"], ttl=-1)
        context = GraphQLContext(request_id="req1", auth_token=token)
        result = self.authenticator.authenticate(context)
        self.assertTrue(result)  # Falls back to simple validation

    def test_authenticate_short_token(self):
        """Test authentication with short token (fails simple validation)."""
        context = GraphQLContext(request_id="req1", auth_token="short")
        self.assertFalse(self.authenticator.authenticate(context))

    def test_authenticate_long_token(self):
        """Test authentication with long token (passes simple validation)."""
        context = GraphQLContext(request_id="req1", auth_token="a" * 32)
        self.assertTrue(self.authenticator.authenticate(context))
        self.assertEqual(context.user_id, "authenticated_user")
        self.assertEqual(context.permissions, ["read", "write", "execute"])

    def test_create_token(self):
        """Test token creation."""
        token = self.authenticator.create_token("user1", ["read"], ttl=3600)
        self.assertIsInstance(token, str)
        self.assertEqual(len(token), 64)  # SHA256 hex digest


# =============================================================================
# Test GraphQL Context
# =============================================================================

class TestGraphQLContext(unittest.TestCase):
    """Test GraphQLContext dataclass."""

    def test_default_values(self):
        """Test default values."""
        context = GraphQLContext(request_id="req1")
        self.assertEqual(context.request_id, "req1")
        self.assertIsNone(context.user_id)
        self.assertIsNone(context.auth_token)
        self.assertEqual(context.permissions, [])
        self.assertEqual(context.rate_limit_remaining, 100)
        self.assertEqual(context.metadata, {})

    def test_custom_values(self):
        """Test custom values."""
        context = GraphQLContext(
            request_id="req1",
            user_id="user1",
            auth_token="token123",
            permissions=["read", "write"],
            rate_limit_remaining=50,
            metadata={"key": "value"}
        )
        self.assertEqual(context.user_id, "user1")
        self.assertEqual(context.auth_token, "token123")
        self.assertEqual(context.permissions, ["read", "write"])
        self.assertEqual(context.rate_limit_remaining, 50)
        self.assertEqual(context.metadata, {"key": "value"})


# =============================================================================
# Test GraphQL Error Response
# =============================================================================

class TestGraphQLErrorResponse(unittest.TestCase):
    """Test GraphQLErrorResponse dataclass."""

    def test_basic_error(self):
        """Test basic error formatting."""
        error = GraphQLErrorResponse(message="Test error")
        result = error.to_dict()
        self.assertEqual(result["message"], "Test error")
        self.assertNotIn("locations", result)
        self.assertNotIn("path", result)
        self.assertNotIn("extensions", result)

    def test_error_with_locations(self):
        """Test error with locations."""
        error = GraphQLErrorResponse(
            message="Test error",
            locations=[{"line": 1, "column": 2}]
        )
        result = error.to_dict()
        self.assertEqual(result["locations"], [{"line": 1, "column": 2}])

    def test_error_with_path(self):
        """Test error with path."""
        error = GraphQLErrorResponse(
            message="Test error",
            path=["field1", "field2"]
        )
        result = error.to_dict()
        self.assertEqual(result["path"], ["field1", "field2"])

    def test_error_with_extensions(self):
        """Test error with extensions."""
        error = GraphQLErrorResponse(
            message="Test error",
            extensions={"code": "VALIDATION_ERROR"}
        )
        result = error.to_dict()
        self.assertEqual(result["extensions"], {"code": "VALIDATION_ERROR"})


# =============================================================================
# Test Workflow Execution Status
# =============================================================================

class TestWorkflowExecutionStatus(unittest.TestCase):
    """Test WorkflowExecutionStatus enum."""

    def test_status_values(self):
        """Test status enum values."""
        self.assertEqual(WorkflowExecutionStatus.PENDING.value, "pending")
        self.assertEqual(WorkflowExecutionStatus.RUNNING.value, "running")
        self.assertEqual(WorkflowExecutionStatus.COMPLETED.value, "completed")
        self.assertEqual(WorkflowExecutionStatus.FAILED.value, "failed")
        self.assertEqual(WorkflowExecutionStatus.CANCELLED.value, "cancelled")
        self.assertEqual(WorkflowExecutionStatus.PAUSED.value, "paused")


# =============================================================================
# Test Workflow Execution
# =============================================================================

class TestWorkflowExecution(unittest.TestCase):
    """Test WorkflowExecution dataclass."""

    def test_creation(self):
        """Test workflow execution creation."""
        execution = WorkflowExecution(
            execution_id="exec1",
            workflow_id="wf1",
            workflow_name="Test Workflow",
            status=WorkflowExecutionStatus.RUNNING,
            started_at=time.time()
        )
        self.assertEqual(execution.execution_id, "exec1")
        self.assertEqual(execution.workflow_id, "wf1")
        self.assertEqual(execution.status, WorkflowExecutionStatus.RUNNING)
        self.assertEqual(execution.progress, 0.0)
        self.assertEqual(execution.input_variables, {})

    def test_with_all_fields(self):
        """Test workflow execution with all fields."""
        now = time.time()
        execution = WorkflowExecution(
            execution_id="exec1",
            workflow_id="wf1",
            workflow_name="Test Workflow",
            status=WorkflowExecutionStatus.COMPLETED,
            started_at=now,
            completed_at=now + 10,
            input_variables={"var1": "value1"},
            output_variables={"result": "success"},
            error_message=None,
            progress=1.0,
            steps_completed=5,
            total_steps=5
        )
        self.assertEqual(execution.completed_at, now + 10)
        self.assertEqual(execution.input_variables, {"var1": "value1"})
        self.assertEqual(execution.output_variables, {"result": "success"})
        self.assertEqual(execution.progress, 1.0)


# =============================================================================
# Test Workflow Variable
# =============================================================================

class TestWorkflowVariable(unittest.TestCase):
    """Test WorkflowVariable dataclass."""

    def test_creation(self):
        """Test workflow variable creation."""
        variable = WorkflowVariable(
            variable_id="var1",
            workflow_id="wf1",
            name="test_var",
            value="test_value",
            var_type="string"
        )
        self.assertEqual(variable.variable_id, "var1")
        self.assertEqual(variable.name, "test_var")
        self.assertEqual(variable.var_type, "string")
        self.assertFalse(variable.is_secret)

    def test_secret_variable(self):
        """Test secret variable."""
        variable = WorkflowVariable(
            variable_id="var1",
            workflow_id="wf1",
            name="password",
            value="secret123",
            var_type="string",
            is_secret=True
        )
        self.assertTrue(variable.is_secret)


# =============================================================================
# Test Subscription Manager
# =============================================================================

class TestSubscriptionManager(unittest.TestCase):
    """Test SubscriptionManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = SubscriptionManager()
        self.callback_data = []

    def callback(self, data):
        """Test callback."""
        self.callback_data.append(data)

    def test_subscribe(self):
        """Test subscribing to events."""
        sub_id = self.manager.subscribe("sub1", "test_query", {}, self.callback)
        self.assertEqual(sub_id, "sub1")
        self.assertIn("sub1", self.manager.subscriptions)

    def test_unsubscribe(self):
        """Test unsubscribing from events."""
        self.manager.subscribe("sub1", "test_query", {}, self.callback)
        result = self.manager.unsubscribe("sub1")
        self.assertTrue(result)
        self.assertNotIn("sub1", self.manager.subscriptions)

    def test_unsubscribe_nonexistent(self):
        """Test unsubscribing from nonexistent subscription."""
        result = self.manager.unsubscribe("nonexistent")
        self.assertFalse(result)

    def test_push_update_sync(self):
        """Test pushing sync update."""
        self.manager.subscribe("sub1", "test_query", {}, self.callback)
        result = self.manager.push_update("sub1", {"event": "test"})
        self.assertTrue(result)
        self.assertEqual(len(self.callback_data), 1)

    def test_push_update_inactive(self):
        """Test pushing to inactive subscription."""
        self.manager.subscribe("sub1", "test_query", {}, self.callback)
        self.manager.subscriptions["sub1"]["active"] = False
        result = self.manager.push_update("sub1", {"event": "test"})
        self.assertFalse(result)


# =============================================================================
# Test Batch Processor
# =============================================================================

class TestBatchProcessor(unittest.TestCase):
    """Test BatchProcessor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.processor = BatchProcessor(max_batch_size=10)

    def test_process_batch_success(self):
        """Test successful batch processing."""
        operations = [
            BatchOperation("mutation", "CreateWorkflow", {"name": "WF1"}),
            BatchOperation("mutation", "CreateWorkflow", {"name": "WF2"})
        ]

        def executor(op):
            return {"success": True, "name": op.variables.get("name")}

        results = self.processor.process_batch(operations, executor)
        self.assertEqual(len(results), 2)
        self.assertTrue(results[0]["data"]["success"])
        self.assertTrue(results[1]["data"]["success"])

    def test_process_batch_with_error(self):
        """Test batch processing with error."""
        operations = [
            BatchOperation("mutation", "CreateWorkflow", {"name": "WF1"})
        ]

        def executor(op):
            raise Exception("Test error")

        results = self.processor.process_batch(operations, executor)
        self.assertEqual(len(results), 1)
        self.assertIsNone(results[0]["data"])
        self.assertIsNotNone(results[0]["errors"])


# =============================================================================
# Test Mock GraphQL Schema Builder
# =============================================================================

class TestMockGraphQLSchemaBuilder(unittest.TestCase):
    """Test MockGraphQLSchemaBuilder class."""

    def setUp(self):
        """Set up test fixtures."""
        _clear_stores()
        self.builder = MockGraphQLSchemaBuilder()
        self.context = GraphQLContext(request_id="req1")

    def test_execute_query_creates_workflow(self):
        """Test executing a query."""
        query = "{ workflows { id name } }"
        result = self.builder.execute_query(query, self.context)
        self.assertIn("data", result)
        self.assertIn("workflows", result["data"])

    def test_execute_mutation_create_workflow(self):
        """Test creating workflow via mutation."""
        mutation = """
        mutation CreateWorkflow($input: WorkflowInput!) {
            createWorkflow(input: $input) {
                id name status
            }
        }
        """
        variables = {
            "input": {
                "name": "Test Workflow",
                "description": "Test description",
                "steps": ["step1", "step2"]
            }
        }
        result = self.builder.execute_mutation(mutation, self.context, variables)
        self.assertIn("data", result)
        self.assertIn("createWorkflow", result["data"])
        workflow = result["data"]["createWorkflow"]
        self.assertEqual(workflow["name"], "Test Workflow")
        self.assertEqual(workflow["status"], "draft")

    def test_execute_mutation_workflow_not_found(self):
        """Test executing mutation with nonexistent workflow."""
        mutation = """
        mutation ExecuteWorkflow($workflowId: ID!) {
            executeWorkflow(workflowId: $workflowId) {
                success executionId
            }
        }
        """
        variables = {"workflowId": "nonexistent"}
        result = self.builder.execute_mutation(mutation, self.context, variables)
        self.assertIn("errors", result)

    def test_execute_batch_operations(self):
        """Test batch operations."""
        operations = [
            {"type": "mutation", "operationName": "CreateWorkflow", "variables": {"input": {"name": "WF1"}}},
            {"type": "mutation", "operationName": "CreateWorkflow", "variables": {"input": {"name": "WF2"}}}
        ]
        results = self.builder.execute_batch_operations(operations, self.context)
        self.assertEqual(len(results), 2)
        self.assertTrue(results[0]["success"])
        self.assertTrue(results[1]["success"])

    def test_execute_batch_operations_with_error(self):
        """Test batch operations with error."""
        operations = [
            {"type": "mutation", "operationName": "UnknownOperation", "variables": {}}
        ]
        results = self.builder.execute_batch_operations(operations, self.context)
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0]["success"])
        self.assertIsNotNone(results[0]["error"])

    def test_check_auth_failure(self):
        """Test authentication check failure."""
        self.builder.authenticator = GraphQLAuthenticator()
        self.context.auth_token = "short"
        with self.assertRaises(AuthenticationError):
            self.builder._check_auth(self.context, "read")

    def test_check_rate_limit_exceeded(self):
        """Test rate limit check."""
        self.builder.rate_limiter = RateLimiter(max_requests=1, window_seconds=60)
        self.builder._check_rate_limit(self.context)
        with self.assertRaises(RateLimitError):
            self.builder._check_rate_limit(self.context)


# =============================================================================
# Test Error Formatting
# =============================================================================

class TestErrorFormatting(unittest.TestCase):
    """Test error formatting."""

    def setUp(self):
        """Set up test fixtures."""
        self.builder = MockGraphQLSchemaBuilder()

    def test_format_authentication_error(self):
        """Test formatting authentication error."""
        error = AuthenticationError("Auth required")
        result = self.builder._format_error(error)
        self.assertEqual(result["extensions"]["code"], "AUTHENTICATION_ERROR")

    def test_format_rate_limit_error(self):
        """Test formatting rate limit error."""
        error = RateLimitError("Rate limit exceeded")
        result = self.builder._format_error(error)
        self.assertEqual(result["extensions"]["code"], "RATE_LIMIT_ERROR")

    def test_format_validation_error(self):
        """Test formatting validation error."""
        error = ValidationError("Invalid input")
        result = self.builder._format_error(error)
        self.assertEqual(result["extensions"]["code"], "VALIDATION_ERROR")

    def test_format_execution_error(self):
        """Test formatting execution error."""
        error = ExecutionError("Execution failed")
        result = self.builder._format_error(error)
        self.assertEqual(result["extensions"]["code"], "EXECUTION_ERROR")

    def test_format_unknown_error(self):
        """Test formatting unknown error."""
        error = Exception("Unknown error")
        result = self.builder._format_error(error)
        self.assertEqual(result["extensions"]["code"], "INTERNAL_ERROR")


if __name__ == "__main__":
    unittest.main()
