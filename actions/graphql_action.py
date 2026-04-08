"""GraphQL action module for RabAI AutoClick.

Provides GraphQL API operations including query execution,
mutation handling, and subscription support.
"""

import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class GraphQLRequest:
    """Represents a GraphQL request.
    
    Attributes:
        query: GraphQL query string.
        variables: Optional variables for the query.
        operation_name: Optional operation name for batched queries.
    """
    query: str
    variables: Optional[Dict[str, Any]] = None
    operation_name: Optional[str] = None


class GraphQLClient:
    """GraphQL client for API operations.
    
    Provides methods for executing queries, mutations,
    and managing GraphQL requests with proper error handling.
    """
    
    def __init__(
        self,
        endpoint: str = "",
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30
    ) -> None:
        """Initialize GraphQL client.
        
        Args:
            endpoint: GraphQL API endpoint URL.
            headers: Optional HTTP headers.
            timeout: Request timeout in seconds.
        """
        self.endpoint = endpoint
        self.headers = headers or {}
        self.timeout = timeout
        self._session: Optional[Any] = None
    
    def connect(self) -> bool:
        """Test connection to GraphQL endpoint.
        
        Returns:
            True if endpoint is reachable, False otherwise.
        """
        try:
            import requests
        except ImportError:
            raise ImportError("requests is required. Install with: pip install requests")
        
        try:
            self._session = requests.Session()
            self._session.headers.update(self.headers)
            
            introspection_query = """
            {
                __schema {
                    queryType { name }
                }
            }
            """
            
            response = self._session.post(
                self.endpoint,
                json={"query": introspection_query},
                timeout=self.timeout
            )
            
            return response.status_code == 200
        
        except Exception:
            self._session = None
            return False
    
    def disconnect(self) -> None:
        """Close the GraphQL session."""
        if self._session:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None
    
    def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute a GraphQL query or mutation.
        
        Args:
            query: GraphQL query string.
            variables: Optional variables dictionary.
            operation_name: Optional operation name.
            
        Returns:
            Response data dictionary.
        """
        if not self._session:
            raise RuntimeError("Not connected to GraphQL endpoint")
        
        try:
            import requests
            
            payload: Dict[str, Any] = {"query": query}
            if variables:
                payload["variables"] = variables
            if operation_name:
                payload["operationName"] = operation_name
            
            response = self._session.post(
                self.endpoint,
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
            
            result = response.json()
            
            if "errors" in result:
                error_messages = [e.get("message", str(e)) for e in result["errors"]]
                raise Exception(f"GraphQL errors: {'; '.join(error_messages)}")
            
            return result.get("data", {})
        
        except ImportError:
            raise ImportError("requests is required")
        except Exception as e:
            raise Exception(f"GraphQL execution failed: {str(e)}")
    
    def query(
        self,
        query_str: str,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a GraphQL query (read operation).
        
        Args:
            query_str: GraphQL query string.
            variables: Optional variables.
            
        Returns:
            Query result data.
        """
        if not query_str.strip().startswith("query"):
            query_str = f"query {query_str}"
        
        return self.execute(query_str, variables)
    
    def mutate(
        self,
        mutation_str: str,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute a GraphQL mutation (write operation).
        
        Args:
            mutation_str: GraphQL mutation string.
            variables: Optional variables.
            
        Returns:
            Mutation result data.
        """
        if not mutation_str.strip().startswith("mutation"):
            mutation_str = f"mutation {mutation_str}"
        
        return self.execute(mutation_str, variables)
    
    def batch_query(
        self,
        queries: List[GraphQLRequest]
    ) -> List[Dict[str, Any]]:
        """Execute multiple queries in a single request.
        
        Args:
            queries: List of GraphQLRequest objects.
            
        Returns:
            List of results for each query.
        """
        if not self._session:
            raise RuntimeError("Not connected to GraphQL endpoint")
        
        try:
            import requests
            
            payloads = []
            for q in queries:
                payload: Dict[str, Any] = {"query": q.query}
                if q.variables:
                    payload["variables"] = q.variables
                if q.operation_name:
                    payload["operationName"] = q.operation_name
                payloads.append(payload)
            
            if len(payloads) == 1:
                result = self.execute(
                    queries[0].query,
                    queries[0].variables,
                    queries[0].operation_name
                )
                return [result]
            
            response = self._session.post(
                self.endpoint,
                json=payloads,
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
            
            results = response.json()
            
            if not isinstance(results, list):
                return [results]
            
            return results
        
        except Exception as e:
            raise Exception(f"Batch query failed: {str(e)}")
    
    def introspect(self) -> Dict[str, Any]:
        """Get the GraphQL schema via introspection.
        
        Returns:
            Full schema introspection result.
        """
        introspection_query = """
        {
            __schema {
                types {
                    name
                    kind
                    description
                    fields {
                        name
                        description
                        type {
                            name
                            kind
                        }
                        args {
                            name
                            description
                            type {
                                name
                                kind
                            }
                            defaultValue
                        }
                    }
                }
                queryType { name fields { name description type { name kind } } }
                mutationType { name fields { name description type { name kind } } }
                subscriptionType { name fields { name description type { name kind } } }
            }
        }
        """
        
        return self.execute(introspection_query)
    
    def get_schema_types(self) -> List[Dict[str, Any]]:
        """Get all types from the schema.
        
        Returns:
            List of type definitions.
        """
        introspection = self.introspect()
        return introspection.get("__schema", {}).get("types", [])
    
    def get_query_fields(self) -> List[Dict[str, Any]]:
        """Get all query fields.
        
        Returns:
            List of query field definitions.
        """
        schema = self.introspect()
        query_type = schema.get("__schema", {}).get("queryType", {})
        return query_type.get("fields", [])
    
    def health_check(self) -> Dict[str, Any]:
        """Check GraphQL endpoint health.
        
        Returns:
            Health status information.
        """
        try:
            query = "{ __typename }"
            self.execute(query)
            return {"healthy": True, "endpoint": self.endpoint}
        except Exception as e:
            return {"healthy": False, "endpoint": self.endpoint, "error": str(e)}


class GraphQLAction(BaseAction):
    """GraphQL action for API query and mutation operations.
    
    Supports executing queries, mutations, batch operations,
    and schema introspection.
    """
    action_type: str = "graphql"
    display_name: str = "GraphQL动作"
    description: str = "GraphQL API查询和变更操作"
    
    def __init__(self) -> None:
        super().__init__()
        self._client: Optional[GraphQLClient] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute GraphQL operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "query":
                return self._query(params, start_time)
            elif operation == "mutate":
                return self._mutate(params, start_time)
            elif operation == "batch":
                return self._batch_query(params, start_time)
            elif operation == "introspect":
                return self._introspect(start_time)
            elif operation == "schema_types":
                return self._schema_types(start_time)
            elif operation == "query_fields":
                return self._query_fields(start_time)
            elif operation == "health":
                return self._health_check(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GraphQL operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to GraphQL endpoint."""
        endpoint = params.get("endpoint", "")
        if not endpoint:
            return ActionResult(success=False, message="endpoint is required", duration=time.time() - start_time)
        
        headers = params.get("headers", {})
        timeout = params.get("timeout", 30)
        
        self._client = GraphQLClient(endpoint=endpoint, headers=headers, timeout=timeout)
        
        success = self._client.connect()
        
        return ActionResult(
            success=success,
            message=f"Connected to {endpoint}" if success else "Failed to connect",
            duration=time.time() - start_time
        )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from GraphQL endpoint."""
        if self._client:
            self._client.disconnect()
            self._client = None
        
        return ActionResult(
            success=True,
            message="Disconnected from GraphQL endpoint",
            duration=time.time() - start_time
        )
    
    def _query(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a GraphQL query."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        query = params.get("query", "")
        variables = params.get("variables")
        
        if not query:
            return ActionResult(success=False, message="query is required", duration=time.time() - start_time)
        
        try:
            result = self._client.query(query, variables)
            return ActionResult(
                success=True,
                message="Query executed successfully",
                data={"data": result},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _mutate(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a GraphQL mutation."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        mutation = params.get("mutation", "")
        variables = params.get("variables")
        
        if not mutation:
            return ActionResult(success=False, message="mutation is required", duration=time.time() - start_time)
        
        try:
            result = self._client.mutate(mutation, variables)
            return ActionResult(
                success=True,
                message="Mutation executed successfully",
                data={"data": result},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _batch_query(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute multiple queries in batch."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        queries_data = params.get("queries", [])
        if not queries_data:
            return ActionResult(success=False, message="queries list is required", duration=time.time() - start_time)
        
        try:
            queries = []
            for q in queries_data:
                queries.append(GraphQLRequest(
                    query=q.get("query", ""),
                    variables=q.get("variables"),
                    operation_name=q.get("operation_name")
                ))
            
            results = self._client.batch_query(queries)
            return ActionResult(
                success=True,
                message=f"Batch executed {len(results)} queries",
                data={"results": results, "count": len(results)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _introspect(self, start_time: float) -> ActionResult:
        """Get GraphQL schema via introspection."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            schema = self._client.introspect()
            return ActionResult(
                success=True,
                message="Schema introspection complete",
                data={"schema": schema},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _schema_types(self, start_time: float) -> ActionResult:
        """Get all schema types."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            types = self._client.get_schema_types()
            return ActionResult(
                success=True,
                message=f"Found {len(types)} types",
                data={"types": types, "count": len(types)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _query_fields(self, start_time: float) -> ActionResult:
        """Get all query fields."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            fields = self._client.get_query_fields()
            return ActionResult(
                success=True,
                message=f"Found {len(fields)} query fields",
                data={"fields": fields, "count": len(fields)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
    
    def _health_check(self, start_time: float) -> ActionResult:
        """Check GraphQL endpoint health."""
        if not self._client:
            return ActionResult(success=False, message="Not connected", duration=time.time() - start_time)
        
        try:
            health = self._client.health_check()
            return ActionResult(
                success=health.get("healthy", False),
                message="Healthy" if health.get("healthy") else "Unhealthy",
                data=health,
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(success=False, message=str(e), duration=time.time() - start_time)
