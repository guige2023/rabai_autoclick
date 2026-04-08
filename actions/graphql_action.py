"""GraphQL action module for RabAI AutoClick.

Provides GraphQL operations:
- GraphQLQueryAction: Execute GraphQL query
- GraphQLMutationAction: Execute GraphQL mutation
- GraphQLSubscriptionAction: Subscribe to GraphQL events
- GraphQLIntrospectionAction: Introspect GraphQL schema
"""

from __future__ import annotations

import sys
import os
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GraphQLQueryAction(BaseAction):
    """Execute GraphQL query."""
    action_type = "graphql_query"
    display_name = "GraphQL查询"
    description = "执行GraphQL查询"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute GraphQL query."""
        endpoint = params.get('endpoint', '')
        query = params.get('query', '')
        variables = params.get('variables', {})
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'graphql_result')

        if not endpoint or not query:
            return ActionResult(success=False, message="endpoint and query are required")

        try:
            import requests
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint
            resolved_query = context.resolve_value(query) if context else query
            resolved_variables = context.resolve_value(variables) if context else variables

            request_headers = {
                'Content-Type': 'application/json',
            }
            request_headers.update(headers or {})

            response = requests.post(
                resolved_endpoint,
                json={'query': resolved_query, 'variables': resolved_variables},
                headers=request_headers,
                timeout=30
            )

            result = response.json()

            return ActionResult(
                success=response.ok and 'errors' not in result,
                data={output_var: result},
                message=result.get('errors', [{}])[0].get('message', 'Success') if 'errors' in result else 'Success'
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL query error: {e}")


class GraphQLMutationAction(BaseAction):
    """Execute GraphQL mutation."""
    action_type = "graphql_mutation"
    display_name = "GraphQL变更"
    description = "执行GraphQL变更"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute GraphQL mutation."""
        endpoint = params.get('endpoint', '')
        mutation = params.get('mutation', '')
        variables = params.get('variables', {})
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'graphql_result')

        if not endpoint or not mutation:
            return ActionResult(success=False, message="endpoint and mutation are required")

        try:
            import requests
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint
            resolved_mutation = context.resolve_value(mutation) if context else mutation

            request_headers = {
                'Content-Type': 'application/json',
            }
            request_headers.update(headers or {})

            response = requests.post(
                resolved_endpoint,
                json={'query': resolved_mutation, 'variables': variables},
                headers=request_headers,
                timeout=30
            )

            result = response.json()

            return ActionResult(
                success=response.ok and 'errors' not in result,
                data={output_var: result},
                message=result.get('errors', [{}])[0].get('message', 'Mutation successful') if 'errors' in result else 'Mutation successful'
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL mutation error: {e}")


class GraphQLSubscriptionAction(BaseAction):
    """Subscribe to GraphQL events."""
    action_type = "graphql_subscription"
    display_name = "GraphQL订阅"
    description = "订阅GraphQL事件"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute GraphQL subscription."""
        endpoint = params.get('endpoint', '')
        subscription = params.get('subscription', '')
        max_messages = params.get('max_messages', 10)
        output_var = params.get('output_var', 'graphql_messages')

        if not endpoint or not subscription:
            return ActionResult(success=False, message="endpoint and subscription are required")

        try:
            import websocket
            messages = []

            def on_message(ws, message):
                import json
                data = json.loads(message)
                messages.append(data)
                if len(messages) >= max_messages:
                    ws.close()

            def on_error(ws, error):
                pass

            def on_close(ws, *args):
                pass

            ws = websocket.WebSocketApp(
                endpoint.replace('http', 'ws'),
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            import threading
            ws_thread = threading.Thread(target=ws.run_forever)
            ws_thread.daemon = True
            ws_thread.start()

            ws.send(subscription)

            import time
            time.sleep(max_messages)

            return ActionResult(
                success=True,
                data={output_var: messages},
                message=f"Received {len(messages)} messages"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL subscription error: {e}")


class GraphQLIntrospectionAction(BaseAction):
    """Introspect GraphQL schema."""
    action_type = "graphql_introspection"
    display_name = "GraphQL内省"
    description = "内省GraphQL schema"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute introspection."""
        endpoint = params.get('endpoint', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'graphql_schema')

        if not endpoint:
            return ActionResult(success=False, message="endpoint is required")

        try:
            import requests

            introspection_query = '''
            query IntrospectionQuery {
                __schema {
                    queryType { name }
                    mutationType { name }
                    subscriptionType { name }
                    types {
                        ...FullType
                    }
                }
            }
            fragment FullType on __Type {
                kind
                name
                fields(includeDeprecated: true) {
                    name
                    args { ...InputValue }
                    type { ...TypeRef }
                    isDeprecated
                    deprecationReason
                }
            }
            fragment InputValue on __InputValue {
                name
                type { ...TypeRef }
                defaultValue
            }
            fragment TypeRef on __Type {
                kind
                name
                ofType { kind name ofType { kind name ofType { kind name } } }
            }
            '''

            request_headers = {'Content-Type': 'application/json'}
            request_headers.update(headers or {})

            response = requests.post(
                endpoint,
                json={'query': introspection_query},
                headers=request_headers,
                timeout=30
            )

            result = response.json()

            if 'data' in result:
                schema_data = result['data']['__schema']
                types = schema_data.get('types', [])
                type_names = [t['name'] for t in types if t['name'] and not t['name'].startswith('__')]

                summary = {
                    'query_type': schema_data.get('queryType', {}).get('name'),
                    'mutation_type': schema_data.get('mutationType', {}).get('name'),
                    'subscription_type': schema_data.get('subscriptionType', {}).get('name'),
                    'type_count': len(type_names),
                    'types': type_names[:50],
                }

                return ActionResult(
                    success=True,
                    data={output_var: summary},
                    message=f"Introspected {len(type_names)} types"
                )

            return ActionResult(success=False, data={output_var: result}, message="Introspection failed")
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL introspection error: {e}")
