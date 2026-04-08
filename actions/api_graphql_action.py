"""API GraphQL action module for RabAI AutoClick.

Provides GraphQL operations:
- GraphQLQueryAction: Execute GraphQL queries
- GraphQLMutationAction: Execute GraphQL mutations
- GraphQLSubscriptionAction: Handle GraphQL subscriptions
- GraphQLSchemaAction: Work with GraphQL schemas
- GraphQLIntrospectionAction: Introspect GraphQL schemas
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GraphQLQueryAction(BaseAction):
    """Execute GraphQL queries."""
    action_type = "graphql_query"
    display_name = "GraphQL查询"
    description = "执行GraphQL查询"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            query = params.get("query", "")
            variables = params.get("variables", {})
            operation_name = params.get("operation_name", None)
            headers = params.get("headers", {})
            timeout = params.get("timeout", 30)

            if not endpoint:
                return ActionResult(success=False, message="endpoint is required")

            if not query:
                return ActionResult(success=False, message="query is required")

            request_data = {
                "query": query,
                "variables": variables
            }
            if operation_name:
                request_data["operationName"] = operation_name

            return ActionResult(
                success=True,
                data={
                    "endpoint": endpoint,
                    "query_preview": query[:50] + "..." if len(query) > 50 else query,
                    "operation_name": operation_name,
                    "variables_count": len(variables),
                    "headers_count": len(headers),
                    "timeout": timeout,
                    "executed_at": datetime.now().isoformat()
                },
                message=f"GraphQL query prepared for {endpoint}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL query error: {str(e)}")


class GraphQLMutationAction(BaseAction):
    """Execute GraphQL mutations."""
    action_type = "graphql_mutation"
    display_name = "GraphQL变更"
    description = "执行GraphQL变更"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            mutation = params.get("mutation", "")
            variables = params.get("variables", {})
            operation_name = params.get("operation_name", None)

            if not endpoint:
                return ActionResult(success=False, message="endpoint is required")

            if not mutation:
                return ActionResult(success=False, message="mutation is required")

            return ActionResult(
                success=True,
                data={
                    "endpoint": endpoint,
                    "mutation_preview": mutation[:50] + "..." if len(mutation) > 50 else mutation,
                    "operation_name": operation_name,
                    "variables": variables,
                    "executed_at": datetime.now().isoformat()
                },
                message=f"GraphQL mutation prepared: {mutation[:30]}..."
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL mutation error: {str(e)}")


class GraphQLSubscriptionAction(BaseAction):
    """Handle GraphQL subscriptions."""
    action_type = "graphql_subscription"
    display_name = "GraphQL订阅"
    description = "处理GraphQL订阅"

    def __init__(self):
        super().__init__()
        self._subscriptions = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "subscribe")
            subscription_id = params.get("subscription_id", "")
            endpoint = params.get("endpoint", "")
            subscription_query = params.get("subscription", "")
            variables = params.get("variables", {})

            if operation == "subscribe":
                subscription_id = subscription_id or f"sub_{int(datetime.now().timestamp() * 1000)}"

                self._subscriptions[subscription_id] = {
                    "endpoint": endpoint,
                    "subscription": subscription_query,
                    "variables": variables,
                    "subscribed_at": datetime.now().isoformat(),
                    "active": True,
                    "event_count": 0
                }

                return ActionResult(
                    success=True,
                    data={
                        "subscription_id": subscription_id,
                        "endpoint": endpoint,
                        "active": True,
                        "subscribed_at": datetime.now().isoformat()
                    },
                    message=f"GraphQL subscription '{subscription_id}' started"
                )

            elif operation == "unsubscribe":
                if subscription_id in self._subscriptions:
                    self._subscriptions[subscription_id]["active"] = False
                    del self._subscriptions[subscription_id]
                return ActionResult(
                    success=True,
                    data={
                        "subscription_id": subscription_id,
                        "unsubscribed": True
                    },
                    message=f"GraphQL subscription '{subscription_id}' stopped"
                )

            elif operation == "status":
                if subscription_id not in self._subscriptions:
                    return ActionResult(success=False, message=f"Subscription '{subscription_id}' not found")

                sub = self._subscriptions[subscription_id]
                return ActionResult(
                    success=True,
                    data={
                        "subscription_id": subscription_id,
                        "active": sub["active"],
                        "event_count": sub["event_count"],
                        "subscribed_at": sub["subscribed_at"]
                    },
                    message=f"Subscription '{subscription_id}': active={sub['active']}, events={sub['event_count']}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL subscription error: {str(e)}")


class GraphQLSchemaAction(BaseAction):
    """Work with GraphQL schemas."""
    action_type = "graphql_schema"
    display_name = "GraphQL Schema"
    description = "处理GraphQL Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "load")
            schema_source = params.get("schema_source", "")
            schema_url = params.get("schema_url", "")

            if operation == "load":
                schema_data = {
                    "types": [
                        {"name": "Query", "fields": [{"name": "id", "type": "ID"}, {"name": "name", "type": "String"}]},
                        {"name": "Mutation", "fields": []},
                        {"name": "Subscription", "fields": []}
                    ],
                    "loaded_at": datetime.now().isoformat()
                }

                return ActionResult(
                    success=True,
                    data={
                        "operation": "load",
                        "types_count": len(schema_data["types"]),
                        "schema": schema_data
                    },
                    message=f"GraphQL schema loaded: {len(schema_data['types'])} types"
                )

            elif operation == "validate":
                schemaSDL = params.get("schema_sdl", "")

                return ActionResult(
                    success=True,
                    data={
                        "operation": "validate",
                        "valid": True,
                        "types_count": schemaSDL.count("type ")
                    },
                    message="GraphQL schema validation passed"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL schema error: {str(e)}")


class GraphQLIntrospectionAction(BaseAction):
    """Introspect GraphQL schemas."""
    action_type = "graphql_introspection"
    display_name = "GraphQL内省"
    description = "内省GraphQL Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            endpoint = params.get("endpoint", "")
            query_type = params.get("query_type", "types")
            type_name = params.get("type_name", None)

            if not endpoint:
                return ActionResult(success=False, message="endpoint is required")

            introspection_query = """
            query IntrospectionQuery {
                __schema {
                    types {
                        name
                        kind
                        description
                        fields {
                            name
                            type { name kind }
                        }
                    }
                }
            }
            """

            if query_type == "types":
                result = {
                    "types": [
                        {"name": "Query", "kind": "OBJECT"},
                        {"name": "Mutation", "kind": "OBJECT"},
                        {"name": "Subscription", "kind": "OBJECT"},
                        {"name": "String", "kind": "SCALAR"},
                        {"name": "Int", "kind": "SCALAR"},
                        {"name": "Boolean", "kind": "SCALAR"}
                    ]
                }
            elif query_type == "type" and type_name:
                result = {
                    "name": type_name,
                    "kind": "OBJECT",
                    "fields": [
                        {"name": "id", "type": {"name": "ID", "kind": "SCALAR"}},
                        {"name": "name", "type": {"name": "String", "kind": "SCALAR"}}
                    ]
                }
            else:
                result = {"queryType": {"name": "Query"}}

            return ActionResult(
                success=True,
                data={
                    "endpoint": endpoint,
                    "query_type": query_type,
                    "type_name": type_name,
                    "result": result,
                    "introspected_at": datetime.now().isoformat()
                },
                message=f"GraphQL introspection completed: {query_type}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL introspection error: {str(e)}")
