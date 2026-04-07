"""GraphQL API client action module for RabAI AutoClick.

Provides GraphQL operations:
- GraphQLQueryAction: Execute GraphQL query
- GraphQLMutationAction: Execute GraphQL mutation
- GraphQLSubscriptionAction: WebSocket subscription
- GraphQLIntrospectionAction: Fetch schema introspection
- GraphQLValidateAction: Validate GraphQL query
"""

from __future__ import annotations

import json
import sys
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
        variables = params.get('variables', None)
        headers = params.get('headers', {})
        operation_name = params.get('operation_name', None)
        output_var = params.get('output_var', 'graphql_result')
        timeout = params.get('timeout', 30)

        if not endpoint or not query:
            return ActionResult(success=False, message="endpoint and query are required")

        try:
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint
            resolved_query = context.resolve_value(query) if context else query
            resolved_variables = context.resolve_value(variables) if context else variables
            resolved_headers = context.resolve_value(headers) if context else headers
            resolved_timeout = context.resolve_value(timeout) if context else timeout

            payload: Dict[str, Any] = {'query': resolved_query}
            if resolved_variables:
                payload['variables'] = resolved_variables
            if operation_name:
                payload['operationName'] = operation_name

            import urllib.request
            req = urllib.request.Request(
                resolved_endpoint,
                data=json.dumps(payload).encode('utf-8'),
                headers={**resolved_headers, 'Content-Type': 'application/json'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=resolved_timeout) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            errors = data.get('errors', [])
            result_data = data.get('data', {})

            if errors:
                return ActionResult(
                    success=False,
                    message=f"GraphQL errors: {errors}",
                    data={'errors': errors}
                )

            if context:
                context.set(output_var, result_data)
            return ActionResult(success=True, message="Query executed", data=result_data)
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL query error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['endpoint', 'query']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'variables': None, 'headers': {}, 'operation_name': None, 'output_var': 'graphql_result', 'timeout': 30}


class GraphQLMutationAction(BaseAction):
    """Execute GraphQL mutation."""
    action_type = "graphql_mutation"
    display_name = "GraphQL变更"
    description = "执行GraphQL变更操作"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute GraphQL mutation."""
        endpoint = params.get('endpoint', '')
        mutation = params.get('mutation', '')
        variables = params.get('variables', None)
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'graphql_mutation_result')
        timeout = params.get('timeout', 30)

        if not endpoint or not mutation:
            return ActionResult(success=False, message="endpoint and mutation are required")

        try:
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint
            resolved_mutation = context.resolve_value(mutation) if context else mutation
            resolved_variables = context.resolve_value(variables) if context else variables
            resolved_headers = context.resolve_value(headers) if context else headers

            payload: Dict[str, Any] = {'query': resolved_mutation}
            if resolved_variables:
                payload['variables'] = resolved_variables

            import urllib.request
            req = urllib.request.Request(
                resolved_endpoint,
                data=json.dumps(payload).encode('utf-8'),
                headers={**resolved_headers, 'Content-Type': 'application/json'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            errors = data.get('errors', [])
            result_data = data.get('data', {})

            if errors:
                return ActionResult(success=False, message=f"Mutation errors: {errors}", data={'errors': errors})

            if context:
                context.set(output_var, result_data)
            return ActionResult(success=True, message="Mutation executed", data=result_data)
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL mutation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['endpoint', 'mutation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'variables': None, 'headers': {}, 'output_var': 'graphql_mutation_result', 'timeout': 30}


class GraphQLIntrospectionAction(BaseAction):
    """Fetch GraphQL schema introspection."""
    action_type = "graphql_introspection"
    display_name = "GraphQL introspection"
    description = "获取GraphQL schema introspection"
    version = "1.0"

    INTROSPECTION_QUERY = '''
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
                    args { name description type { name kind ofType { name kind } } }
                    type { name kind ofType { name kind } }
                    isDeprecated
                    deprecationReason
                }
                inputFields { name description type { name kind ofType { name kind } } }
                enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason }
            }
        }
    }
    '''

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute introspection query."""
        endpoint = params.get('endpoint', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'graphql_schema')

        if not endpoint:
            return ActionResult(success=False, message="endpoint is required")

        try:
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint
            resolved_headers = context.resolve_value(headers) if context else headers

            payload = {'query': self.INTROSPECTION_QUERY}
            import urllib.request
            req = urllib.request.Request(
                resolved_endpoint,
                data=json.dumps(payload).encode('utf-8'),
                headers={**resolved_headers, 'Content-Type': 'application/json'},
                method='POST'
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode('utf-8'))

            errors = data.get('errors', [])
            if errors:
                return ActionResult(success=False, message=f"Introspection errors: {errors}")

            schema = data.get('data', {}).get('__schema', {})
            if context:
                context.set(output_var, schema)
            return ActionResult(success=True, message="Schema introspection fetched", data={'types_count': len(schema.get('types', []))})
        except Exception as e:
            return ActionResult(success=False, message=f"Introspection error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['endpoint']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'graphql_schema'}


class GraphQLValidateAction(BaseAction):
    """Validate GraphQL query syntax."""
    action_type = "graphql_validate"
    display_name = "GraphQL验证"
    description = "验证GraphQL查询语法"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate GraphQL query."""
        query = params.get('query', '')
        schema = params.get('schema', None)

        if not query:
            return ActionResult(success=False, message="query is required")

        try:
            resolved_query = context.resolve_value(query) if context else query

            # Basic syntax validation - check for balanced braces
            open_braces = resolved_query.count('{')
            close_braces = resolved_query.count('}')
            if open_braces != close_braces:
                return ActionResult(success=False, message="Unbalanced braces in query")

            # Check for common GraphQL keywords
            valid_starts = ('query', 'mutation', 'subscription', '{', 'fragment', 'query ', 'mutation ', 'subscription ')
            if not any(resolved_query.strip().startswith(kw) for kw in valid_starts):
                return ActionResult(success=False, message="Query must start with query, mutation, subscription, or {")

            if context:
                context.set('graphql_valid', True)
            return ActionResult(success=True, message="GraphQL query syntax appears valid")
        except Exception as e:
            return ActionResult(success=False, message=f"Validation error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['query']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'schema': None}


class GraphQLBatchAction(BaseAction):
    """Execute batch GraphQL operations."""
    action_type = "graphql_batch"
    display_name = "GraphQL批量操作"
    description = "批量执行GraphQL操作"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute batch GraphQL operations."""
        endpoint = params.get('endpoint', '')
        operations = params.get('operations', [])
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'graphql_batch_result')

        if not endpoint or not operations:
            return ActionResult(success=False, message="endpoint and operations are required")

        try:
            resolved_endpoint = context.resolve_value(endpoint) if context else endpoint
            resolved_ops = context.resolve_value(operations) if context else operations

            results = []
            import urllib.request

            for op in resolved_ops:
                query = op.get('query', '')
                variables = op.get('variables', {})
                operation_name = op.get('operation_name')

                payload: Dict[str, Any] = {'query': query}
                if variables:
                    payload['variables'] = variables
                if operation_name:
                    payload['operationName'] = operation_name

                req = urllib.request.Request(
                    resolved_endpoint,
                    data=json.dumps(payload).encode('utf-8'),
                    headers={**headers, 'Content-Type': 'application/json'},
                    method='POST'
                )
                try:
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        data = json.loads(resp.read().decode('utf-8'))
                        results.append({'success': True, 'data': data.get('data'), 'errors': data.get('errors', [])})
                except Exception as e:
                    results.append({'success': False, 'error': str(e)})

            if context:
                context.set(output_var, results)
            return ActionResult(success=True, message=f"Batch completed: {len(results)} operations", data={'results': results})
        except Exception as e:
            return ActionResult(success=False, message=f"GraphQL batch error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['endpoint', 'operations']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'graphql_batch_result'}
