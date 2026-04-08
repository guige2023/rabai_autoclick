"""GraphQL action module for RabAI AutoClick.

Provides GraphQL query and mutation execution with
query building, variable support, and response parsing.
"""

import sys
import os
import json
import time
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GraphQLAction(BaseAction):
    """Execute GraphQL queries and mutations.
    
    Supports query building, variable substitution,
    header configuration, and response extraction.
    """
    action_type = "graphql"
    display_name = "GraphQL请求"
    description = "执行GraphQL查询和变更，支持变量和响应提取"

    VALID_OPERATIONS = ["query", "mutation", "subscription"]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a GraphQL operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - endpoint: str, GraphQL endpoint URL
                - query: str, GraphQL query/mutation string
                - variables: dict, query variables
                - operation_name: str, optional operation name
                - headers: dict, HTTP headers
                - timeout: int, request timeout
                - extract_field: str, field path to extract from response
                - save_to_var: str, output variable
        
        Returns:
            ActionResult with GraphQL response data.
        """
        endpoint = params.get('endpoint', '')
        query = params.get('query', '')
        variables = params.get('variables', {})
        operation_name = params.get('operation_name', None)
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        extract_field = params.get('extract_field', None)
        save_to_var = params.get('save_to_var', None)

        if not endpoint:
            return ActionResult(success=False, message="endpoint is required")
        if not query:
            return ActionResult(success=False, message="query is required")

        start_time = time.time()

        # Build request payload
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        if operation_name:
            payload["operationName"] = operation_name

        # Set default headers
        default_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        default_headers.update(headers)

        try:
            data = json.dumps(payload).encode('utf-8')
            request = Request(endpoint, data=data, headers=default_headers, method='POST')

            with urlopen(request, timeout=timeout) as response:
                response_body = response.read().decode('utf-8')
                response_data = json.loads(response_body)

        except HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP Error {e.code}: {e.reason}",
                data={"error": str(e), "code": e.code},
                duration=time.time() - start_time
            )
        except URLError as e:
            return ActionResult(
                success=False,
                message=f"URL Error: {e.reason}",
                data={"error": str(e)},
                duration=time.time() - start_time
            )
        except json.JSONDecodeError as e:
            return ActionResult(
                success=False,
                message=f"JSON decode error: {e}",
                data={"raw_response": response_body if 'response_body' in dir() else None},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GraphQL request failed: {e}",
                duration=time.time() - start_time
            )

        # Check for GraphQL errors
        if "errors" in response_data and response_data["errors"]:
            error_messages = [err.get("message", str(err)) for err in response_data["errors"]]
            return ActionResult(
                success=False,
                message=f"GraphQL errors: {'; '.join(error_messages)}",
                data=response_data,
                duration=time.time() - start_time
            )

        # Extract specific field if requested
        result_data = response_data.get("data")
        if extract_field and result_data is not None:
            result_data = self._extract_field(result_data, extract_field)

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result_data

        return ActionResult(
            success=True,
            message="GraphQL request successful",
            data=result_data,
            duration=time.time() - start_time
        )

    def _extract_field(self, data: Any, field_path: str) -> Any:
        """Extract nested field using dot notation."""
        parts = field_path.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                idx = int(part)
                current = current[idx] if idx < len(current) else None
            else:
                return None
            if current is None:
                return None
        return current

    def build_query(
        self,
        operation_type: str,
        operation_name: str,
        fields: List[str],
        variables: Optional[Dict[str, Any]] = None,
        arguments: Optional[Dict[str, str]] = None
    ) -> str:
        """Build a GraphQL query string.
        
        Args:
            operation_type: 'query' or 'mutation'
            operation_name: Name of the operation
            fields: List of field names to select
            variables: Variable definitions {name: type}
            arguments: Argument mappings {field: arg_string}
        
        Returns:
            GraphQL query string.
        """
        var_defs = ""
        if variables:
            var_parts = [f"${name}: {vtype}" for name, vtype in variables.items()]
            var_defs = f"({', '.join(var_parts)})"

        args_strs = []
        if arguments:
            for field_name, arg_str in arguments.items():
                args_strs.append(f"{field_name}({arg_str})")
        elif variables:
            for var_name in variables.keys():
                args_strs.append(f"{var_name}: ${var_name}")

        field_str = '\n'.join(f"  {f}" for f in fields)
        selection = '\n'.join(args_strs) + f" {{\n{field_str}\n  }}" if args_strs else field_str

        query = f"{operation_type} {operation_name}{var_defs} {{\n  {selection}\n}}"
        return query

    def get_required_params(self) -> List[str]:
        return ['endpoint', 'query']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'variables': {},
            'operation_name': None,
            'headers': {},
            'timeout': 30,
            'extract_field': None,
            'save_to_var': None,
        }


class GraphQLIntrospectionAction(BaseAction):
    """Fetch GraphQL schema introspection.
    
    Retrieves the full schema from a GraphQL endpoint.
    """
    action_type = "graphql_introspection"
    display_name = "GraphQL introspection"
    description = "获取GraphQL schema introspection"

    INTROSPECTION_QUERY = """
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

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Fetch schema introspection.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - endpoint: str, GraphQL endpoint
                - headers: dict
                - timeout: int
                - save_to_var: str
        
        Returns:
            ActionResult with schema data.
        """
        endpoint = params.get('endpoint', '')
        headers = params.get('headers', {})
        timeout = params.get('timeout', 30)
        save_to_var = params.get('save_to_var', None)

        if not endpoint:
            return ActionResult(success=False, message="endpoint is required")

        start_time = time.time()

        default_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        default_headers.update(headers)

        payload = {"query": self.INTROSPECTION_QUERY}
        try:
            data = json.dumps(payload).encode('utf-8')
            request = Request(endpoint, data=data, headers=default_headers, method='POST')
            with urlopen(request, timeout=timeout) as response:
                response_body = response.read().decode('utf-8')
                result = json.loads(response_body)
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Introspection failed: {e}",
                duration=time.time() - start_time
            )

        if "errors" in result:
            return ActionResult(
                success=False,
                message=f"Introspection errors: {result['errors']}",
                data=result,
                duration=time.time() - start_time
            )

        if save_to_var and hasattr(context, 'vars'):
            context.vars[save_to_var] = result.get("data")

        return ActionResult(
            success=True,
            message="Schema introspection fetched",
            data=result.get("data"),
            duration=time.time() - start_time
        )

    def get_required_params(self) -> List[str]:
        return ['endpoint']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'headers': {}, 'timeout': 30, 'save_to_var': None}
