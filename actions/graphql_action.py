"""GraphQL action module for RabAI AutoClick.

Provides GraphQL operations:
- GraphQLQueryAction: Execute GraphQL query
- GraphQLMutateAction: Execute GraphQL mutation
- GraphQLSubscribeAction: Execute GraphQL subscription
- GraphQLIntrospectAction: Introspect GraphQL schema
"""

import json
from typing import Any, Dict, List, Optional

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class GraphQLQueryAction(BaseAction):
    """Execute GraphQL query."""
    action_type = "graphql_query"
    display_name = "GraphQL查询"
    description = "执行GraphQL查询"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute query.

        Args:
            context: Execution context.
            params: Dict with endpoint, query, variables, headers, output_var.

        Returns:
            ActionResult with query result.
        """
        endpoint = params.get('endpoint', '')
        query = params.get('query', '')
        variables = params.get('variables', {})
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'graphql_result')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(endpoint, str, 'endpoint')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(query, str, 'query')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_endpoint = context.resolve_value(endpoint)
            resolved_query = context.resolve_value(query)
            resolved_vars = context.resolve_value(variables) if variables else {}
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_timeout = context.resolve_value(timeout)

            payload = {
                'query': resolved_query,
                'variables': resolved_vars
            }

            encoded_body = json.dumps(payload).encode('utf-8')

            request = urllib.request.Request(
                resolved_endpoint,
                data=encoded_body,
                method='POST'
            )
            request.add_header('Content-Type', 'application/json')

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                response_body = json.loads(resp.read().decode('utf-8'))

            if 'errors' in response_body:
                return ActionResult(
                    success=False,
                    message=f"GraphQL错误: {response_body['errors']}",
                    data={'errors': response_body['errors']}
                )

            data = response_body.get('data', {})
            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"GraphQL查询完成",
                data={'data': data, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GraphQL查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['endpoint', 'query']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'variables': {}, 'headers': {}, 'output_var': 'graphql_result', 'timeout': 30}


class GraphQLMutateAction(BaseAction):
    """Execute GraphQL mutation."""
    action_type = "graphql_mutate"
    display_name = "GraphQL变更"
    description = "执行GraphQL变更"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute mutation.

        Args:
            context: Execution context.
            params: Dict with endpoint, mutation, variables, headers, output_var.

        Returns:
            ActionResult with mutation result.
        """
        endpoint = params.get('endpoint', '')
        mutation = params.get('mutation', '')
        variables = params.get('variables', {})
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'graphql_result')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(endpoint, str, 'endpoint')
        if not valid:
            return ActionResult(success=False, message=msg)

        valid, msg = self.validate_type(mutation, str, 'mutation')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_endpoint = context.resolve_value(endpoint)
            resolved_mutation = context.resolve_value(mutation)
            resolved_vars = context.resolve_value(variables) if variables else {}
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_timeout = context.resolve_value(timeout)

            payload = {
                'query': resolved_mutation,
                'variables': resolved_vars
            }

            encoded_body = json.dumps(payload).encode('utf-8')

            request = urllib.request.Request(
                resolved_endpoint,
                data=encoded_body,
                method='POST'
            )
            request.add_header('Content-Type', 'application/json')

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                response_body = json.loads(resp.read().decode('utf-8'))

            if 'errors' in response_body:
                return ActionResult(
                    success=False,
                    message=f"GraphQL错误: {response_body['errors']}",
                    data={'errors': response_body['errors']}
                )

            data = response_body.get('data', {})
            context.set(output_var, data)

            return ActionResult(
                success=True,
                message=f"GraphQL变更完成",
                data={'data': data, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GraphQL变更失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['endpoint', 'mutation']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'variables': {}, 'headers': {}, 'output_var': 'graphql_result', 'timeout': 30}


class GraphQLIntrospectAction(BaseAction):
    """Introspect GraphQL schema."""
    action_type = "graphql_introspect"
    display_name = "GraphQL内省"
    description = "获取GraphQL schema"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute introspection.

        Args:
            context: Execution context.
            params: Dict with endpoint, headers, output_var.

        Returns:
            ActionResult with schema.
        """
        endpoint = params.get('endpoint', '')
        headers = params.get('headers', {})
        output_var = params.get('output_var', 'graphql_schema')
        timeout = params.get('timeout', 30)

        valid, msg = self.validate_type(endpoint, str, 'endpoint')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            import urllib.request

            resolved_endpoint = context.resolve_value(endpoint)
            resolved_headers = context.resolve_value(headers) if headers else {}
            resolved_timeout = context.resolve_value(timeout)

            introspection_query = '''
            {
              __schema {
                queryType { name }
                mutationType { name }
                subscriptionType { name }
                types {
                  kind
                  name
                  fields(includeDeprecated: true) {
                    name
                    args { name type { name } }
                    type { name kind }
                    isDeprecated
                    deprecationReason
                  }
                }
              }
            }
            '''

            payload = {'query': introspection_query}
            encoded_body = json.dumps(payload).encode('utf-8')

            request = urllib.request.Request(
                resolved_endpoint,
                data=encoded_body,
                method='POST'
            )
            request.add_header('Content-Type', 'application/json')

            for k, v in resolved_headers.items():
                request.add_header(k, str(v))

            with urllib.request.urlopen(request, timeout=int(resolved_timeout)) as resp:
                response_body = json.loads(resp.read().decode('utf-8'))

            if 'errors' in response_body:
                return ActionResult(
                    success=False,
                    message=f"GraphQL内省失败: {response_body['errors']}",
                    data={'errors': response_body['errors']}
                )

            schema = response_body.get('data', {}).get('__schema', {})
            context.set(output_var, schema)

            types = schema.get('types', [])
            type_count = len([t for t in types if not t['name'].startswith('__')])

            return ActionResult(
                success=True,
                message=f"GraphQL内省完成: {type_count} 类型",
                data={'type_count': type_count, 'output_var': output_var}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GraphQL内省失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['endpoint']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'output_var': 'graphql_schema', 'timeout': 30}
