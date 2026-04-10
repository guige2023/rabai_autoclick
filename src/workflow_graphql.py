"""
GraphQL API Layer for Workflow Automation v24
P0级功能 - GraphQL API with queries, mutations, subscriptions, batch operations,
introspection, authentication, rate limiting, schema stitching
"""
import json
import time
import uuid
import asyncio
import hashlib
from typing import Dict, List, Optional, Any, Callable, AsyncGenerator
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
from functools import wraps
import threading
import re

# GraphQL Core imports (standard Python GraphQL library)
try:
    from graphql import (
        GraphQLSchema, GraphQLObjectType, GraphQLField, GraphQLFieldSet,
        GraphQLArgument, GraphQLInputObjectType, GraphQLInputObjectField,
        GraphQLEnumType, GraphQLList, GraphQLNonNull, GraphQLString,
        GraphQLInt, GraphQLFloat, GraphQLBoolean, GraphQLID,
        GraphQLScalarType, GraphQLInputType, GraphQLOutputType,
        GraphQLAbstractType, GraphQLIsTypeOfFn, GraphQLTypeResolverFn,
        ExecutionContext, ExecutionResult, parse, validate, execute, subscribe,
        get_introspection_query, IntrospectionSchema, IntrospectionQuery,
        IntrospectionObjectType, IntrospectionField, IntrospectionInputValue,
        IntrospectionEnumValue, IntrospectionType, IntrospectionFullType,
        build_schema, extend_schema, lexicographic_sort_schema,
        GraphQLError, GraphQLFormattedError, formatted_error,
        DEFAULT_OPERATION_RESOLVERS
    )
    from graphql.execution import Middleware
    GRAPHQL_AVAILABLE = True
except ImportError:
    GRAPHQL_AVAILABLE = False

from .workflow_share import WorkflowShareLink, ShareType


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


class RateLimiter:
    """Token bucket rate limiter for GraphQL operations"""

    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: Dict[str, List[float]] = defaultdict(list)
        self._lock = threading.Lock()

    def check_rate_limit(self, key: str, cost: int = 1) -> bool:
        """Check if request is within rate limit"""
        with self._lock:
            now = time.time()
            window_start = now - self.window_seconds

            # Clean old requests
            self.requests[key] = [t for t in self.requests[key] if t > window_start]

            if len(self.requests[key]) >= self.max_requests:
                return False

            self.requests[key].append(now)
            return True

    def get_remaining(self, key: str) -> int:
        """Get remaining requests for key"""
        with self._lock:
            now = time.time()
            window_start = now - self.window_seconds
            self.requests[key] = [t for t in self.requests[key] if t > window_start]
            return max(0, self.max_requests - len(self.requests[key]))


class GraphQLAuthenticator:
    """GraphQL-specific authentication handler"""

    def __init__(self, secret_key: str = "default-secret"):
        self.secret_key = secret_key
        self.token_cache: Dict[str, Dict[str, Any]] = {}

    def authenticate(self, context: GraphQLContext) -> bool:
        """Authenticate GraphQL request"""
        if not context.auth_token:
            # For demo, allow if no token (open API)
            return True

        # Validate token
        token_hash = hashlib.sha256(context.auth_token.encode()).hexdigest()
        if token_hash in self.token_cache:
            token_data = self.token_cache[token_hash]
            if token_data.get("expires", 0) > time.time():
                context.user_id = token_data.get("user_id")
                context.permissions = token_data.get("permissions", [])
                return True

        # Simple token validation (in production, use JWT)
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


class WorkflowExecutionStatus(Enum):
    """Workflow execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


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
    var_type: str  # string, integer, float, boolean, json
    is_secret: bool = False


class SchemaStitcher:
    """Combines multiple GraphQL schemas"""

    def __init__(self):
        self.schemas: Dict[str, GraphQLSchema] = {}
        self.type_extensions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def add_schema(self, name: str, schema: GraphQLSchema):
        """Add a schema to be stitched"""
        self.schemas[name] = schema

    def stitch(self) -> GraphQLSchema:
        """Combine all schemas into one"""
        if not self.schemas:
            raise ValueError("No schemas to stitch")

        if len(self.schemas) == 1:
            return list(self.schemas.values())[0]

        # Merge query types
        merged_query = self._merge_query_types()
        merged_mutation = self._merge_mutation_types()
        merged_subscription = self._merge_subscription_types()

        return GraphQLSchema(
            query=merged_query,
            mutation=merged_mutation if merged_mutation else None,
            subscription=merged_subscription if merged_subscription else None
        )

    def _merge_query_types(self) -> GraphQLObjectType:
        """Merge all query types"""
        fields = {}
        for name, schema in self.schemas.items():
            if schema.query_type:
                for field_name, field_obj in schema.query_type.fields.items():
                    fields[f"{name}_{field_name}"] = field_obj
        return GraphQLObjectType("Query", fields=fields)

    def _merge_mutation_types(self) -> Optional[GraphQLObjectType]:
        """Merge all mutation types"""
        fields = {}
        for name, schema in self.schemas.items():
            if schema.mutation_type:
                for field_name, field_obj in schema.mutation_type.fields.items():
                    fields[f"{name}_{field_name}"] = field_obj
        if not fields:
            return None
        return GraphQLObjectType("Mutation", fields=fields)

    def _merge_subscription_types(self) -> Optional[GraphQLObjectType]:
        """Merge all subscription types"""
        fields = {}
        for name, schema in self.schemas.items():
            if schema.subscription_type:
                for field_name, field_obj in schema.subscription_type.fields.items():
                    fields[f"{name}_{field_name}"] = field_obj
        if not fields:
            return None
        return GraphQLObjectType("Subscription", fields=fields)


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
        executor: Callable[[BatchOperation], Any]
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
        callback: Callable[[Dict[str, Any]], None]
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


# In-memory data stores (in production, use database)
_workflows_store: Dict[str, Dict[str, Any]] = {}
_executions_store: Dict[str, WorkflowExecution] = {}
_variables_store: Dict[str, WorkflowVariable] = {}


class GraphQLSchemaBuilder:
    """Builds the complete GraphQL schema"""

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
            raise RateLimitError(
                f"Rate limit exceeded. Remaining: {remaining}"
            )

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

    def _create_workflow_type(self) -> GraphQLObjectType:
        """Create Workflow GraphQL type"""
        return GraphQLObjectType(
            "Workflow",
            fields=lambda: {
                "id": GraphQLField(GraphQLNonNull(GraphQLID)),
                "name": GraphQLField(GraphQLNonNull(GraphQLString)),
                "description": GraphQLField(GraphQLString),
                "version": GraphQLField(GraphQLString),
                "status": GraphQLField(GraphQLString),
                "createdAt": GraphQLField(GraphQLFloat),
                "updatedAt": GraphQLField(GraphQLFloat),
                "createdBy": GraphQLField(GraphQLString),
                "steps": GraphQLField(GraphQLList(GraphQLString)),
                "variables": GraphQLField(GraphQLList(GraphQLString)),
                "metadata": GraphQLField(GraphQLString),  # JSON string
            }
        )

    def _create_execution_type(self) -> GraphQLObjectType:
        """Create Execution GraphQL type"""
        return GraphQLObjectType(
            "Execution",
            fields=lambda: {
                "executionId": GraphQLField(GraphQLNonNull(GraphQLID)),
                "workflowId": GraphQLField(GraphQLNonNull(GraphQLID)),
                "workflowName": GraphQLField(GraphQLNonNull(GraphQLString)),
                "status": GraphQLField(GraphQLNonNull(GraphQLString)),
                "startedAt": GraphQLField(GraphQLFloat),
                "completedAt": GraphQLField(GraphQLFloat),
                "progress": GraphQLField(GraphQLFloat),
                "stepsCompleted": GraphQLField(GraphQLInt),
                "totalSteps": GraphQLField(GraphQLInt),
                "inputVariables": GraphQLField(GraphQLString),  # JSON
                "outputVariables": GraphQLField(GraphQLString),  # JSON
                "errorMessage": GraphQLField(GraphQLString),
            }
        )

    def _create_variable_type(self) -> GraphQLObjectType:
        """Create Variable GraphQL type"""
        return GraphQLObjectType(
            "Variable",
            fields=lambda: {
                "variableId": GraphQLField(GraphQLNonNull(GraphQLID)),
                "workflowId": GraphQLField(GraphQLNonNull(GraphQLID)),
                "name": GraphQLField(GraphQLNonNull(GraphQLString)),
                "value": GraphQLField(GraphQLString),
                "type": GraphQLField(GraphQLNonNull(GraphQLString)),
                "isSecret": GraphQLField(GraphQLBoolean),
            }
        )

    def _create_execution_result_type(self) -> GraphQLObjectType:
        """Create ExecutionResult type"""
        return GraphQLObjectType(
            "ExecutionResult",
            fields=lambda: {
                "success": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
                "executionId": GraphQLField(GraphQLID),
                "output": GraphQLField(GraphQLString),  # JSON
                "error": GraphQLField(GraphQLString),
                "executionTime": GraphQLField(GraphQLFloat),
            }
        )

    def _create_subscription_event_type(self) -> GraphQLObjectType:
        """Create SubscriptionEvent type"""
        return GraphQLObjectType(
            "SubscriptionEvent",
            fields=lambda: {
                "eventType": GraphQLField(GraphQLNonNull(GraphQLString)),
                "timestamp": GraphQLField(GraphQLFloat),
                "data": GraphQLField(GraphQLString),  # JSON
                "workflowId": GraphQLField(GraphQLID),
                "executionId": GraphQLField(GraphQLID),
            }
        )

    def _create_batch_result_type(self) -> GraphQLObjectType:
        """Create BatchResult type"""
        return GraphQLObjectType(
            "BatchResult",
            fields=lambda: {
                "index": GraphQLField(GraphQLNonNull(GraphQLInt)),
                "success": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
                "data": GraphQLField(GraphQLString),  # JSON
                "error": GraphQLField(GraphQLString),
            }
        )

    def _build_query_type(self) -> GraphQLObjectType:
        """Build Query type"""
        workflow_type = self._create_workflow_type()
        execution_type = self._create_execution_type()
        variable_type = self._create_variable_type()

        def resolve_workflows(root, info, first=None, skip=None, filter=None):
            context = info.context
            self._check_auth(context, "read")
            self._check_rate_limit(context)

            results = list(_workflows_store.values())
            if filter:
                results = [w for w in results if filter.get("status") and w.get("status") == filter["status"]]

            if skip:
                results = results[skip:]
            if first:
                results = results[:first]
            return results

        def resolve_workflow(root, info, id):
            context = info.context
            self._check_auth(context, "read")
            self._check_rate_limit(context)

            return _workflows_store.get(id)

        def resolve_executions(root, info, workflowId=None, status=None, first=None, skip=None):
            context = info.context
            self._check_auth(context, "read")
            self._check_rate_limit(context)

            results = list(_executions_store.values())
            if workflowId:
                results = [e for e in results if e.workflow_id == workflowId]
            if status:
                results = [e for e in results if e.status.value == status]

            results = sorted(results, key=lambda x: x.started_at, reverse=True)
            if skip:
                results = results[skip:]
            if first:
                results = results[:first]
            return results

        def resolve_execution(root, info, executionId):
            context = info.context
            self._check_auth(context, "read")
            self._check_rate_limit(context)

            return _executions_store.get(executionId)

        def resolve_variables(root, info, workflowId=None):
            context = info.context
            self._check_auth(context, "read")
            self._check_rate_limit(context)

            results = list(_variables_store.values())
            if workflowId:
                results = [v for v in results if v.workflow_id == workflowId]
            return results

        def resolve_variable(root, info, variableId):
            context = info.context
            self._check_auth(context, "read")
            self._check_rate_limit(context)

            return _variables_store.get(variableId)

        def resolve_workflow_execution_stats(root, info, workflowId):
            context = info.context
            self._check_auth(context, "read")
            self._check_rate_limit(context)

            executions = [e for e in _executions_store.values() if e.workflow_id == workflowId]
            if not executions:
                return {
                    "totalExecutions": 0,
                    "successfulExecutions": 0,
                    "failedExecutions": 0,
                    "averageExecutionTime": 0,
                    "successRate": 0
                }

            successful = [e for e in executions if e.status == WorkflowExecutionStatus.COMPLETED]
            failed = [e for e in executions if e.status == WorkflowExecutionStatus.FAILED]

            total_time = 0
            for e in executions:
                if e.completed_at:
                    total_time += e.completed_at - e.started_at

            return {
                "totalExecutions": len(executions),
                "successfulExecutions": len(successful),
                "failedExecutions": len(failed),
                "averageExecutionTime": total_time / len(executions) if executions else 0,
                "successRate": len(successful) / len(executions) if executions else 0
            }

        return GraphQLObjectType(
            "Query",
            fields=lambda: {
                # Workflow queries
                "workflows": GraphQLField(
                    GraphQLList(workflow_type),
                    args={
                        "first": GraphQLArgument(GraphQLInt),
                        "skip": GraphQLArgument(GraphQLInt),
                        "filter": GraphQLArgument(GraphQLString),  # JSON filter
                    },
                    resolve=resolve_workflows
                ),
                "workflow": GraphQLField(
                    workflow_type,
                    args={"id": GraphQLArgument(GraphQLNonNull(GraphQLID))},
                    resolve=resolve_workflow
                ),
                # Execution queries
                "executions": GraphQLField(
                    GraphQLList(execution_type),
                    args={
                        "workflowId": GraphQLArgument(GraphQLID),
                        "status": GraphQLArgument(GraphQLString),
                        "first": GraphQLArgument(GraphQLInt),
                        "skip": GraphQLArgument(GraphQLInt),
                    },
                    resolve=resolve_executions
                ),
                "execution": GraphQLField(
                    execution_type,
                    args={"executionId": GraphQLArgument(GraphQLNonNull(GraphQLID))},
                    resolve=resolve_execution
                ),
                "workflowExecutionStats": GraphQLField(
                    GraphQLString,  # JSON
                    args={"workflowId": GraphQLArgument(GraphQLNonNull(GraphQLID))},
                    resolve=resolve_workflow_execution_stats
                ),
                # Variable queries
                "variables": GraphQLField(
                    GraphQLList(variable_type),
                    args={"workflowId": GraphQLArgument(GraphQLID)},
                    resolve=resolve_variables
                ),
                "variable": GraphQLField(
                    variable_type,
                    args={"variableId": GraphQLArgument(GraphQLNonNull(GraphQLID))},
                    resolve=resolve_variable
                ),
            }
        )

    def _build_mutation_type(self) -> GraphQLObjectType:
        """Build Mutation type"""
        workflow_type = self._create_workflow_type()
        execution_result_type = self._create_execution_result_type()
        batch_result_type = self._create_batch_result_type()

        def resolve_create_workflow(root, info, input):
            context = info.context
            self._check_auth(context, "write")
            self._check_rate_limit(context, cost=5)

            workflow_id = str(uuid.uuid4())
            now = time.time()

            workflow_data = {
                "id": workflow_id,
                "name": input.get("name"),
                "description": input.get("description", ""),
                "version": "1.0.0",
                "status": "draft",
                "createdAt": now,
                "updatedAt": now,
                "createdBy": context.user_id or "anonymous",
                "steps": input.get("steps", []),
                "variables": input.get("variables", []),
                "metadata": input.get("metadata", "{}"),
            }

            _workflows_store[workflow_id] = workflow_data
            return workflow_data

        def resolve_update_workflow(root, info, id, input):
            context = info.context
            self._check_auth(context, "write")
            self._check_rate_limit(context, cost=5)

            if id not in _workflows_store:
                raise ValidationError(f"Workflow {id} not found")

            workflow = _workflows_store[id]
            workflow["updatedAt"] = time.time()

            if input.get("name"):
                workflow["name"] = input["name"]
            if input.get("description"):
                workflow["description"] = input["description"]
            if input.get("steps"):
                workflow["steps"] = input["steps"]
            if input.get("variables"):
                workflow["variables"] = input["variables"]
            if input.get("metadata"):
                workflow["metadata"] = input["metadata"]

            return workflow

        def resolve_delete_workflow(root, info, id):
            context = info.context
            self._check_auth(context, "write")
            self._check_rate_limit(context, cost=5)

            if id not in _workflows_store:
                raise ValidationError(f"Workflow {id} not found")

            del _workflows_store[id]
            return {"success": True, "id": id}

        def resolve_execute_workflow(root, info, workflowId, inputVariables=None):
            context = info.context
            self._check_auth(context, "execute")
            self._check_rate_limit(context, cost=10)

            if workflowId not in _workflows_store:
                raise ExecutionError(f"Workflow {workflowId} not found")

            workflow = _workflows_store[workflowId]
            execution_id = str(uuid.uuid4())
            now = time.time()

            execution = WorkflowExecution(
                execution_id=execution_id,
                workflow_id=workflowId,
                workflow_name=workflow["name"],
                status=WorkflowExecutionStatus.RUNNING,
                started_at=now,
                input_variables=input_variables or {},
                total_steps=len(workflow.get("steps", []))
            )

            _executions_store[execution_id] = execution

            # Simulate async execution
            def run_execution():
                time.sleep(0.1)  # Simulate work
                execution.status = WorkflowExecutionStatus.COMPLETED
                execution.completed_at = time.time()
                execution.progress = 1.0
                execution.steps_completed = execution.total_steps
                execution.output_variables = {"result": "success"}

                # Push subscription update
                asyncio.create_task(
                    self.subscription_manager.push_update(
                        f"execution_{execution_id}",
                        {
                            "eventType": "EXECUTION_COMPLETED",
                            "timestamp": time.time(),
                            "data": json.dumps(asdict(execution)),
                            "workflowId": workflowId,
                            "executionId": execution_id
                        }
                    )
                )

            threading.Thread(target=run_execution, daemon=True).start()

            return {
                "success": True,
                "executionId": execution_id,
                "output": json.dumps({"status": "started"}),
                "error": None,
                "executionTime": 0
            }

        def resolve_cancel_execution(root, info, executionId):
            context = info.context
            self._check_auth(context, "execute")
            self._check_rate_limit(context, cost=5)

            if executionId not in _executions_store:
                raise ExecutionError(f"Execution {executionId} not found")

            execution = _executions_store[executionId]
            execution.status = WorkflowExecutionStatus.CANCELLED
            execution.completed_at = time.time()

            return {
                "success": True,
                "executionId": executionId,
                "output": json.dumps({"status": "cancelled"}),
                "error": None,
                "executionTime": execution.completed_at - execution.started_at
            }

        def resolve_create_variable(root, info, input):
            context = info.context
            self._check_auth(context, "write")
            self._check_rate_limit(context, cost=3)

            variable_id = str(uuid.uuid4())

            variable = WorkflowVariable(
                variable_id=variable_id,
                workflow_id=input["workflowId"],
                name=input["name"],
                value=input.get("value"),
                var_type=input.get("type", "string"),
                is_secret=input.get("isSecret", False)
            )

            _variables_store[variable_id] = variable
            return variable

        def resolve_update_variable(root, info, variableId, input):
            context = info.context
            self._check_auth(context, "write")
            self._check_rate_limit(context, cost=3)

            if variableId not in _variables_store:
                raise ValidationError(f"Variable {variableId} not found")

            variable = _variables_store[variableId]
            if input.get("name"):
                variable.name = input["name"]
            if "value" in input:
                variable.value = input["value"]
            if input.get("type"):
                variable.var_type = input["type"]
            if "isSecret" in input:
                variable.is_secret = input["isSecret"]

            return variable

        def resolve_delete_variable(root, info, variableId):
            context = info.context
            self._check_auth(context, "write")
            self._check_rate_limit(context, cost=3)

            if variableId not in _variables_store:
                raise ValidationError(f"Variable {variableId} not found")

            del _variables_store[variableId]
            return {"success": True, "variableId": variableId}

        def resolve_batch_operations(root, info, operations):
            context = info.context
            self._check_auth(context, "write")
            self._check_rate_limit(context, cost=len(operations) * 2)

            results = []
            for idx, op in enumerate(operations):
                try:
                    op_type = op.get("type")
                    op_name = op.get("operationName")
                    op_variables = op.get("variables", {})

                    # Route to appropriate resolver based on operation name
                    if op_name == "CreateWorkflow":
                        result = resolve_create_workflow(root, info, op_variables.get("input", {}))
                    elif op_name == "UpdateWorkflow":
                        result = resolve_update_workflow(root, info, op_variables.get("id"), op_variables.get("input", {}))
                    elif op_name == "DeleteWorkflow":
                        result = resolve_delete_workflow(root, info, op_variables.get("id"))
                    elif op_name == "ExecuteWorkflow":
                        result = resolve_execute_workflow(root, info, op_variables.get("workflowId"), op_variables.get("inputVariables"))
                    elif op_name == "CreateVariable":
                        result = resolve_create_variable(root, info, op_variables.get("input", {}))
                    else:
                        raise ValidationError(f"Unknown operation: {op_name}")

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

        return GraphQLObjectType(
            "Mutation",
            fields=lambda: {
                # Workflow mutations
                "createWorkflow": GraphQLField(
                    workflow_type,
                    args={"input": GraphQLArgument(GraphQLNonNull(GraphQLString))},  # JSON
                    resolve=resolve_create_workflow
                ),
                "updateWorkflow": GraphQLField(
                    workflow_type,
                    args={
                        "id": GraphQLArgument(GraphQLNonNull(GraphQLID)),
                        "input": GraphQLArgument(GraphQLNonNull(GraphQLString)),  # JSON
                    },
                    resolve=resolve_update_workflow
                ),
                "deleteWorkflow": GraphQLField(
                    GraphQLString,  # JSON
                    args={"id": GraphQLArgument(GraphQLNonNull(GraphQLID))},
                    resolve=resolve_delete_workflow
                ),
                # Execution mutations
                "executeWorkflow": GraphQLField(
                    execution_result_type,
                    args={
                        "workflowId": GraphQLArgument(GraphQLNonNull(GraphQLID)),
                        "inputVariables": GraphQLArgument(GraphQLString),  # JSON
                    },
                    resolve=resolve_execute_workflow
                ),
                "cancelExecution": GraphQLField(
                    execution_result_type,
                    args={"executionId": GraphQLArgument(GraphQLNonNull(GraphQLID))},
                    resolve=resolve_cancel_execution
                ),
                # Variable mutations
                "createVariable": GraphQLField(
                    self._create_variable_type(),
                    args={"input": GraphQLArgument(GraphQLNonNull(GraphQLString))},  # JSON
                    resolve=resolve_create_variable
                ),
                "updateVariable": GraphQLField(
                    self._create_variable_type(),
                    args={
                        "variableId": GraphQLArgument(GraphQLNonNull(GraphQLID)),
                        "input": GraphQLArgument(GraphQLNonNull(GraphQLString)),  # JSON
                    },
                    resolve=resolve_update_variable
                ),
                "deleteVariable": GraphQLField(
                    GraphQLString,  # JSON
                    args={"variableId": GraphQLArgument(GraphQLNonNull(GraphQLID))},
                    resolve=resolve_delete_variable
                ),
                # Batch operations
                "batchOperations": GraphQLField(
                    GraphQLList(batch_result_type),
                    args={"operations": GraphQLArgument(GraphQLNonNull(GraphQLString))},  # JSON array
                    resolve=resolve_batch_operations
                ),
            }
        )

    def _build_subscription_type(self) -> GraphQLObjectType:
        """Build Subscription type"""
        event_type = self._create_subscription_event_type()

        async def resolve_execution_updates(root, info, workflowId=None):
            """Subscribe to execution updates"""
            subscription_id = f"execution_{uuid.uuid4()}"

            async def callback(event_data):
                return event_data

            self.subscription_manager.subscribe(
                subscription_id,
                "executionUpdates",
                {"workflowId": workflowId},
                callback
            )

            yield {"executionUpdates": {"eventType": "SUBSCRIBED", "timestamp": time.time(), "data": "{}"}}

            # In production, this would be a real async generator
            # For demo, we simulate periodic updates
            for _ in range(10):
                await asyncio.sleep(2)
                yield {
                    "executionUpdates": {
                        "eventType": "HEARTBEAT",
                        "timestamp": time.time(),
                        "data": json.dumps({"status": "alive"}),
                        "workflowId": workflowId,
                        "executionId": None
                    }
                }

        async def resolve_workflow_updates(root, info, eventTypes=None):
            """Subscribe to workflow updates"""
            subscription_id = f"workflow_{uuid.uuid4()}"

            async def callback(event_data):
                return event_data

            self.subscription_manager.subscribe(
                subscription_id,
                "workflowUpdates",
                {"eventTypes": eventTypes},
                callback
            )

            yield {"workflowUpdates": {"eventType": "SUBSCRIBED", "timestamp": time.time(), "data": "{}"}}

            for _ in range(10):
                await asyncio.sleep(3)
                yield {
                    "workflowUpdates": {
                        "eventType": "WORKFLOW_MODIFIED",
                        "timestamp": time.time(),
                        "data": json.dumps({"action": "heartbeat"}),
                        "workflowId": None,
                        "executionId": None
                    }
                }

        return GraphQLObjectType(
            "Subscription",
            fields=lambda: {
                "executionUpdates": GraphQLField(
                    event_type,
                    args={"workflowId": GraphQLArgument(GraphQLID)},
                    resolve=resolve_execution_updates
                ),
                "workflowUpdates": GraphQLField(
                    event_type,
                    args={"eventTypes": GraphQLArgument(GraphQLList(GraphQLString))},
                    resolve=resolve_workflow_updates
                ),
            }
        )

    def build_schema(self) -> GraphQLSchema:
        """Build complete GraphQL schema with introspection"""
        query_type = self._build_query_type()
        mutation_type = self._build_mutation_type()
        subscription_type = self._build_subscription_type()

        return GraphQLSchema(
            query=query_type,
            mutation=mutation_type,
            subscription=subscription_type,
            types=[]  # Additional types would go here
        )


class GraphQLAPIServer:
    """GraphQL API Server wrapper"""

    def __init__(
        self,
        max_depth: int = 10,
        max_complexity: int = 1000,
        max_directives: int = 100
    ):
        self.max_depth = max_depth
        self.max_complexity = max_complexity
        self.max_directives = max_directives
        self.schema_builder = GraphQLSchemaBuilder()
        self.schema = self.schema_builder.build_schema()
        self.authenticator = self.schema_builder.authenticator
        self.rate_limiter = self.schema_builder.rate_limiter
        self.subscription_manager = self.schema_builder.subscription_manager

    def execute_query(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None,
        context: Optional[GraphQLContext] = None
    ) -> Dict[str, Any]:
        """Execute a GraphQL query"""
        if context is None:
            context = GraphQLContext(
                request_id=str(uuid.uuid4()),
                auth_token=None
            )

        try:
            # Parse and validate
            document = parse(query)
            errors = validate(self.schema, document)
            if errors:
                return {
                    "data": None,
                    "errors": [self._format_error(e) for e in errors]
                }

            # Execute
            result = execute(
                self.schema,
                document,
                variable_values=variables or {},
                operation_name=operation_name,
                context_value=context
            )

            return {
                "data": result.data,
                "errors": [self._format_error(e) for e in result.errors] if result.errors else None
            }

        except Exception as e:
            return {
                "data": None,
                "errors": [self._format_error(e)]
            }

    def execute_batch(
        self,
        operations: List[Dict[str, Any]],
        context: Optional[GraphQLContext] = None
    ) -> List[Dict[str, Any]]:
        """Execute multiple operations in batch"""
        if context is None:
            context = GraphQLContext(
                request_id=str(uuid.uuid4()),
                auth_token=None
            )

        results = []
        for op in operations:
            query = op.get("query")
            variables = op.get("variables", {})
            operation_name = op.get("operationName")

            result = self.execute_query(query, variables, operation_name, context)
            results.append(result)

        return results

    def get_introspection(self) -> Dict[str, Any]:
        """Get GraphQL introspection result"""
        introspection_query = """
        query IntrospectionQuery {
            __schema {
                queryType { name }
                mutationType { name }
                subscriptionType { name }
                types {
                    ...FullType
                }
                directives {
                    name
                    description
                    locations
                    args {
                        ...InputValue
                    }
                }
            }
        }

        fragment FullType on __Type {
            kind
            name
            description
            fields(includeDeprecated: true) {
                name
                description
                args {
                    ...InputValue
                }
                type {
                    ...TypeRef
                }
                isDeprecated
                deprecationReason
            }
            inputFields {
                ...InputValue
            }
            interfaces {
                ...TypeRef
            }
            enumValues(includeDeprecated: true) {
                name
                description
                isDeprecated
                deprecationReason
            }
            possibleTypes {
                ...TypeRef
            }
        }

        fragment InputValue on __InputValue {
            name
            description
            type {
                ...TypeRef
            }
            defaultValue
        }

        fragment TypeRef on __Type {
            kind
            name
            ofType {
                kind
                name
                ofType {
                    kind
                    name
                    ofType {
                        kind
                        name
                        ofType {
                            kind
                            name
                            ofType {
                                kind
                                name
                                ofType {
                                    kind
                                    name
                                    ofType {
                                        kind
                                        name
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        result = self.execute_query(introspection_query)
        return result.get("data", {})

    def _format_error(self, error: Exception) -> Dict:
        """Format error"""
        return self.schema_builder._format_error(error)

    def stitch_schema(self, other_schema: GraphQLSchema, name: str = "external") -> GraphQLSchema:
        """Stitch another schema into this one"""
        stitcher = SchemaStitcher()
        stitcher.add_schema("main", self.schema)
        stitcher.add_schema(name, other_schema)
        return stitcher.stitch()


# Standalone functions for direct use
def create_graphql_server(
    auth_token: Optional[str] = None,
    max_requests: int = 100,
    window_seconds: int = 60
) -> GraphQLAPIServer:
    """Create a configured GraphQL API server"""
    authenticator = GraphQLAuthenticator()
    rate_limiter = RateLimiter(max_requests, window_seconds)

    builder = GraphQLSchemaBuilder(authenticator, rate_limiter)
    server = GraphQLAPIServer()
    server.schema_builder = builder
    server.schema = builder.build_schema()
    server.authenticator = authenticator
    server.rate_limiter = rate_limiter
    server.subscription_manager = builder.subscription_manager

    return server


# Demo workflow data initialization
def init_demo_data():
    """Initialize demo data for testing"""
    workflow_id = str(uuid.uuid4())
    now = time.time()

    _workflows_store[workflow_id] = {
        "id": workflow_id,
        "name": "Demo Workflow",
        "description": "A demo workflow for testing",
        "version": "1.0.0",
        "status": "active",
        "createdAt": now,
        "updatedAt": now,
        "createdBy": "system",
        "steps": ["step1", "step2", "step3"],
        "variables": ["var1", "var2"],
        "metadata": "{}"
    }

    # Add demo execution
    exec_id = str(uuid.uuid4())
    _executions_store[exec_id] = WorkflowExecution(
        execution_id=exec_id,
        workflow_id=workflow_id,
        workflow_name="Demo Workflow",
        status=WorkflowExecutionStatus.COMPLETED,
        started_at=now - 60,
        completed_at=now - 30,
        progress=1.0,
        steps_completed=3,
        total_steps=3
    )


# Initialize demo data on module load
init_demo_data()
